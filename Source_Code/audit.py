from typing import Tuple

import pandas as pd


class AuditEngine:
    """Engine that runs audit checks across different data sources (Salto / Sesam)."""

    # ------------------------------------------------------------------
    # Public entry points
    # ------------------------------------------------------------------
    def run_salto_audit(self, gold_file: str, salto_file: str, output_file: str) -> Tuple[dict, pd.DataFrame]:
        """Run the audit checks for Salto (matches Gold and Salto data)."""
        df_gold = self._load_source(gold_file)
        df_salto = self._load_source(salto_file)
        df_gold, df_salto = self._normalize_columns(df_gold, df_salto)

        df_salto_agg = self._aggregate_salto(df_salto)
        df_merged = self._merge_gold_salto(df_gold, df_salto_agg)
        df_result, metadata = self._produce_salto_results(df_merged)
        error_df = self._extract_errors(df_result)

        self._write_output(output_file, df_result, metadata, error_df)
        return metadata, error_df

    def run_sesam_audit(self, gold_file: str, sesam_file: str, output_file: str) -> Tuple[dict, pd.DataFrame]:
        """Run the audit checks for Sesam (matches Gold and Sesam data)."""
        df_gold = self._load_source(gold_file)
        df_sesam = self._load_source(sesam_file)
        df_gold, df_sesam = self._normalize_columns(df_gold, df_sesam)

        df_merged = self._merge_gold_sesam(df_gold, df_sesam)
        df_result, metadata = self._produce_sesam_results(df_merged)
        error_df = self._extract_errors(df_result)

        self._write_output(output_file, df_result, metadata, error_df)
        return metadata, error_df

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------
    def _load_source(self, path: str) -> pd.DataFrame:
        if path.lower().endswith(".csv"):
            return pd.read_csv(path)
        return pd.read_excel(path)

    def _normalize_columns(self, df1: pd.DataFrame, df2: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """Normalize column names to lowercase + stripped for consistent processing."""
        df1 = df1.copy()
        df2 = df2.copy()
        df1.columns = df1.columns.str.strip().str.lower()
        df2.columns = df2.columns.str.strip().str.lower()
        return df1, df2

    def _aggregate_salto(self, df_salto: pd.DataFrame) -> pd.DataFrame:
        """Aggregate Salto data so that each Gold Order Reference is unique."""
        for col in ["stc completion date", "cct delivery date", "eqpt delivery date", "stamp date"]:
            if col in df_salto.columns:
                df_salto[col] = pd.to_datetime(df_salto[col], errors="coerce").dt.date

        agg_map = {}
        if "stc completion date" in df_salto.columns:
            agg_map["stc completion date"] = "min"
        if "stc reference" in df_salto.columns:
            agg_map["stc reference"] = "first"
        if "cct delivery date" in df_salto.columns:
            agg_map["cct delivery date"] = "min"
        if "eqpt delivery date" in df_salto.columns:
            agg_map["eqpt delivery date"] = "min"
        if "stamp date" in df_salto.columns:
            agg_map["stamp date"] = "min"

        if "gold order reference" not in df_salto.columns:
            raise ValueError("Salto source file must contain 'gold order reference' column.")

        df_agg = df_salto.groupby("gold order reference").agg(agg_map).reset_index()
        return df_agg

    def _merge_gold_salto(self, df_gold: pd.DataFrame, df_salto_agg: pd.DataFrame) -> pd.DataFrame:
        """Merge GOLD and aggregated Salto data."""
        for col in ["auto close order cmpltd", "ready for service cmpltd", "ready for billing cmpltd"]:
            if col in df_gold.columns:
                df_gold[col] = pd.to_datetime(df_gold[col], errors="coerce").dt.date

        if "order number" not in df_gold.columns:
            raise ValueError("GOLD source file must contain 'order number' column.")
        if "gold order reference" not in df_salto_agg.columns:
            raise ValueError("Aggregated Salto data must contain 'gold order reference' column.")

        merged = pd.merge(
            df_gold,
            df_salto_agg,
            left_on="order number",
            right_on="gold order reference",
            how="inner",
        )
        return merged

    def _merge_gold_sesam(self, df_gold: pd.DataFrame, df_sesam: pd.DataFrame) -> pd.DataFrame:
        """Merge GOLD and Sesam data."""
        for col in ["auto close order cmpltd", "ready for billing cmpltd", "ready for service cmpltd"]:
            if col in df_gold.columns:
                df_gold[col] = pd.to_datetime(df_gold[col], errors="coerce").dt.date

        for col in ["cut_date", "cct delivery date", "eqpt delivery date", "stamp date"]:
            if col in df_sesam.columns:
                df_sesam[col] = pd.to_datetime(df_sesam[col], errors="coerce").dt.date

        if "order number" not in df_gold.columns or "order number" not in df_sesam.columns:
            raise ValueError("Both GOLD and Sesam source files must contain 'order number' column.")

        merged = pd.merge(df_gold, df_sesam, on="order number", how="inner")
        return merged

    def _produce_salto_results(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, dict]:
        """Generate result rows and metadata for the Salto workflow."""
        def gold_closed_salto_open(row):
            ac = row.get("auto close order cmpltd")
            stc = row.get("stc completion date")
            if pd.notnull(ac) and pd.notnull(stc):
                diff = (stc - ac).days
                return 0 if diff > 30 else 1
            return "NA"

        def rfs_rfb(row):
            rfs = row.get("ready for service cmpltd")
            rfb = row.get("ready for billing cmpltd")
            if pd.notnull(rfs) and pd.notnull(rfb):
                return 1 if rfs == rfb else 0
            return "NA"

        def circuit_sync(row):
            cct = row.get("cct delivery date")
            eqpt = row.get("eqpt delivery date")
            if pd.notnull(cct) and pd.notnull(eqpt):
                if cct > eqpt:
                    diff = (cct - eqpt).days
                    return 1 if diff > 30 else 0
                return 0
            return "NA"

        def cct_stamp(row):
            cct = row.get("cct delivery date")
            stamp = row.get("stamp date")
            if pd.notnull(cct) and pd.notnull(stamp):
                diff = abs((stamp - cct).days)
                return 1 if diff <= 10 else 0
            return "NA"

        df = df.copy()
        df["gold closed/salto open after 1 month"] = df.apply(gold_closed_salto_open, axis=1)
        df["rfs=rfb for standard customers"] = df.apply(rfs_rfb, axis=1)
        df["circuit equipment synchronization"] = df.apply(circuit_sync, axis=1)
        df["cct vs stamp date check"] = df.apply(cct_stamp, axis=1)

        output = df[[
            "gold order reference",
            "stc reference",
            "gold closed/salto open after 1 month",
            "rfs=rfb for standard customers",
            "circuit equipment synchronization",
            "cct vs stamp date check",
        ]].rename(columns={
            "gold order reference": "Order Number",
            "stc reference": "STC Reference",
            "gold closed/salto open after 1 month": "Gold Closed/Salto open after 1 month",
            "rfs=rfb for standard customers": "RFS=RFB for Standard Customers",
            "circuit equipment synchronization": "Circuit Equipment Synchronization",
            "cct vs stamp date check": "CCT vs Stamp Date Check",
        })

        metadata = self._compute_summary(output)
        return output, metadata

    def _produce_sesam_results(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, dict]:
        """Generate result rows and metadata for the Sesam workflow."""
        def gold_closed_sesam_open(row):
            ac = row.get("auto close order cmpltd")
            cut = row.get("cut_date")
            if pd.notnull(ac) and pd.notnull(cut):
                return 0 if (cut - ac).days > 30 else 1
            return "NA"

        def circuit_sync(row):
            cct = row.get("cct delivery date")
            eqpt = row.get("eqpt delivery date")
            if pd.notnull(cct) and pd.notnull(eqpt):
                if cct > eqpt:
                    diff = (cct - eqpt).days
                    return 1 if diff > 30 else 0
                return 0
            return "NA"

        def cav_updated(row):
            cct = row.get("cct delivery date")
            stamp = row.get("stamp date")
            if pd.notnull(cct) and pd.notnull(stamp):
                diff = abs((cct - stamp).days)
                return 1 if diff <= 10 else 0
            return "NA"

        def rfs_rfb(row):
            rfs = row.get("ready for service cmpltd")
            rfb = row.get("ready for billing cmpltd")
            if pd.notnull(rfs) and pd.notnull(rfb):
                return 1 if rfs == rfb else 0
            return "NA"

        df = df.copy()
        df["gold closed/sesam salto open after 1 month"] = df.apply(gold_closed_sesam_open, axis=1)
        df["circuit equipment synchronization"] = df.apply(circuit_sync, axis=1)
        df["cav updated within 10 days of availability"] = df.apply(cav_updated, axis=1)
        df["rfs=rfb for standard customers"] = df.apply(rfs_rfb, axis=1)

        output = df[[
            "order number",
            "sesam order ref",
            "gold closed/sesam salto open after 1 month",
            "circuit equipment synchronization",
            "cav updated within 10 days of availability",
            "rfs=rfb for standard customers",
        ]].rename(columns={
            "order number": "Order Number",
            "sesam order ref": "Sesam Order Ref",
            "gold closed/sesam salto open after 1 month": "Gold Closed/Sesam Salto open after 1 month",
            "circuit equipment synchronization": "Circuit Equipment Synchronization",
            "cav updated within 10 days of availability": "CAV Updated Within 10 Days",
            "rfs=rfb for standard customers": "RFS=RFB for Standard Customers",
        })

        metadata = self._compute_summary(output)
        return output, metadata

    def _compute_summary(self, df: pd.DataFrame) -> dict:
        """Compute simple summary metrics for a result dataframe."""
        summary = {"total_rows": len(df)}
        # Count each metric as 1/0/NA
        for col in df.columns:
            if col in {"Order Number", "STC Reference", "Sesam Order Ref"}:
                continue
            counts = df[col].value_counts(dropna=False)
            summary[f"{col} - 1"] = int(counts.get(1, 0))
            summary[f"{col} - 0"] = int(counts.get(0, 0))
            summary[f"{col} - NA"] = int(counts.get("NA", 0))
        return summary

    def _extract_errors(self, df: pd.DataFrame) -> pd.DataFrame:
        """Extract rows that failed one or more validation checks."""
        # Treat 0 as a failure; ignore columns that are identifiers.
        validation_cols = [c for c in df.columns if c not in {"Order Number", "STC Reference", "Sesam Order Ref"}]
        if not validation_cols:
            return pd.DataFrame()

        # Old workflows used DataFrame.applymap, but some pandas/compat layers may not support this.
        try:
            is_error = df[validation_cols].eq(0).any(axis=1)
        except Exception:
            is_error = df[validation_cols].apply(lambda row: (row == 0).any(), axis=1)

        return df.loc[is_error].copy()

    def _write_output(self, output_file: str, df_result: pd.DataFrame, metadata: dict, error_df: pd.DataFrame):
        """Write the output Excel file with a summary sheet and an error log."""
        with pd.ExcelWriter(output_file, engine="openpyxl") as writer:
            df_result.to_excel(writer, index=False, sheet_name="Audit Results")
            pd.DataFrame([metadata]).T.to_excel(writer, sheet_name="Summary")
            if not error_df.empty:
                error_df.to_excel(writer, index=False, sheet_name="Errors")
