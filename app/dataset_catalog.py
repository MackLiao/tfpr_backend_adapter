from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class SupplementalDatasetConfig:
    """Additional config to load alongside a selected primary dataset."""

    config_name: str
    db_name: str
    sample_id_field: str = "sample_id"


@dataclass(frozen=True)
class DatasetCatalogItem:
    """Catalog entry describing a selectable dataset config."""

    id: str
    name: str
    repo_id: str
    config_name: str
    db_name: str
    sample_id_field: str = "sample_id"
    supplemental_configs: tuple[SupplementalDatasetConfig, ...] = ()
    selectable: bool = True
    unsupported_reason: str | None = None


# Curated from BrentLab HuggingFace datacards (annotated_features datasets).
DATASET_CATALOG: tuple[DatasetCatalogItem, ...] = (
    DatasetCatalogItem(
        id="hackett",
        name="Hackett 2020",
        repo_id="BrentLab/hackett_2020",
        config_name="hackett_2020",
        db_name="hackett",
    ),
    DatasetCatalogItem(
        id="kemmeren",
        name="Kemmeren 2014",
        repo_id="BrentLab/kemmeren_2014",
        config_name="kemmeren_2014",
        db_name="kemmeren",
    ),
    DatasetCatalogItem(
        id="harbison",
        name="Harbison 2004",
        repo_id="BrentLab/harbison_2004",
        config_name="harbison_2004",
        db_name="harbison",
    ),
    DatasetCatalogItem(
        id="hughes_overexpression",
        name="Hughes 2006 Overexpression",
        repo_id="BrentLab/hughes_2006",
        config_name="overexpression",
        db_name="hughes_overexpression",
    ),
    DatasetCatalogItem(
        id="hughes_knockout",
        name="Hughes 2006 Knockout",
        repo_id="BrentLab/hughes_2006",
        config_name="knockout",
        db_name="hughes_knockout",
    ),
    DatasetCatalogItem(
        id="hu_reimand",
        name="Hu 2007 / Reimand 2010",
        repo_id="BrentLab/hu_2007_reimand_2010",
        config_name="hu_2007_reimand_2010",
        db_name="hu_reimand",
        selectable=False,
        unsupported_reason=(
            "Current datacard validation fails in tfbpapi for this repository"
        ),
    ),
    DatasetCatalogItem(
        id="mahendrawada_chec",
        name="Mahendrawada 2025 ChEC",
        repo_id="BrentLab/mahendrawada_2025",
        config_name="mahendrawada_chec_seq",
        db_name="mahendrawada_chec",
    ),
    DatasetCatalogItem(
        id="mahendrawada_chec_replicates",
        name="Mahendrawada 2025 ChEC Replicates",
        repo_id="BrentLab/mahendrawada_2025",
        config_name="chec_mahendrawada_m2025_af_replicates",
        db_name="mahendrawada_chec_replicates",
        sample_id_field="sra_accession",
        supplemental_configs=(
            SupplementalDatasetConfig(
                config_name="chec_genome_map_meta",
                db_name="mahendrawada_chec_replicates_regmeta",
                sample_id_field="sra_accession",
            ),
        ),
    ),
    DatasetCatalogItem(
        id="mahendrawada_chec_combined",
        name="Mahendrawada 2025 ChEC Combined",
        repo_id="BrentLab/mahendrawada_2025",
        config_name="chec_mahendrawada_m2025_af_combined",
        db_name="mahendrawada_chec_combined",
        supplemental_configs=(
            SupplementalDatasetConfig(
                config_name="chec_mahendrawada_m2025_af_combined_meta",
                db_name="mahendrawada_chec_combined_regmeta",
            ),
        ),
    ),
    DatasetCatalogItem(
        id="mahendrawada_rna",
        name="Mahendrawada 2025 RNA-seq",
        repo_id="BrentLab/mahendrawada_2025",
        config_name="rna_seq",
        db_name="mahendrawada_rna",
    ),
    DatasetCatalogItem(
        id="mahendrawada_rna_reprocessed",
        name="Mahendrawada 2025 RNA-seq Reprocessed",
        repo_id="BrentLab/mahendrawada_2025",
        config_name="rnaseq_reprocessed",
        db_name="mahendrawada_rna_reprocessed",
    ),
    DatasetCatalogItem(
        id="mahendrawada_degron",
        name="Mahendrawada 2025 Degron Counts",
        repo_id="BrentLab/mahendrawada_2025",
        config_name="degron_counts",
        db_name="mahendrawada_degron",
        sample_id_field="sra_accession",
        supplemental_configs=(
            SupplementalDatasetConfig(
                config_name="degron_counts_meta",
                db_name="mahendrawada_degron_regmeta",
                sample_id_field="sra_accession",
            ),
        ),
    ),
    DatasetCatalogItem(
        id="mahendrawada_mnase_fusion",
        name="Mahendrawada 2025 MNase Fusion RNA-seq",
        repo_id="BrentLab/mahendrawada_2025",
        config_name="mnase_fusion_rnaseq_counts",
        db_name="mahendrawada_mnase_fusion",
        sample_id_field="sra_accession",
        supplemental_configs=(
            SupplementalDatasetConfig(
                config_name="mnase_fusion_rnaseq_counts_meta",
                db_name="mahendrawada_mnase_fusion_regmeta",
                sample_id_field="sra_accession",
            ),
        ),
    ),
    DatasetCatalogItem(
        id="mahendrawada_wt_baseline",
        name="Mahendrawada 2025 WT Baseline Counts",
        repo_id="BrentLab/mahendrawada_2025",
        config_name="wt_baseline_counts",
        db_name="mahendrawada_wt_baseline",
        sample_id_field="sra_accession",
    ),
    DatasetCatalogItem(
        id="mahendrawada_wt_degron_control",
        name="Mahendrawada 2025 WT Degron Control",
        repo_id="BrentLab/mahendrawada_2025",
        config_name="wt_degron_control_counts",
        db_name="mahendrawada_wt_degron_control",
        sample_id_field="sra_accession",
    ),
    DatasetCatalogItem(
        id="rossi_replicates",
        name="Rossi 2021 Replicates",
        repo_id="BrentLab/rossi_2021",
        config_name="rossi_2021_af_replicates",
        db_name="rossi_replicates",
        supplemental_configs=(
            SupplementalDatasetConfig(
                config_name="rossi_2021_metadata",
                db_name="rossi_replicates_regmeta",
            ),
        ),
    ),
    DatasetCatalogItem(
        id="rossi_combined",
        name="Rossi 2021 Combined",
        repo_id="BrentLab/rossi_2021",
        config_name="rossi_2021_af_combined",
        db_name="rossi_combined",
        supplemental_configs=(
            SupplementalDatasetConfig(
                config_name="rossi_2021_metadata_sample",
                db_name="rossi_combined_regmeta",
            ),
        ),
    ),
)


DATASET_CATALOG_BY_ID = {item.id: item for item in DATASET_CATALOG}
DATASET_CATALOG_BY_DB_NAME = {item.db_name: item for item in DATASET_CATALOG}

MANAGED_DATASET_KEYS = (
    {
        (item.repo_id, item.config_name)
        for item in DATASET_CATALOG
        if item.selectable
    }
    | {
        (item.repo_id, supplemental.config_name)
        for item in DATASET_CATALOG
        if item.selectable
        for supplemental in item.supplemental_configs
    }
)
