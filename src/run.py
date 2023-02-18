from dagster import job, config_from_files, file_relative_path
from src.ops.hgnc_ops import get_hgnc_op, get_hgnc_additional_op, concat_hgnc_additional_op, format_hgnc_asset
from src.utils.io import create_output_folders
from src.ops.drh_ops import (
    get_drh_drugs_op,
    get_drh_samples_op,
    merge_drh_op,
    transform_drh_annotations_op,
    transform_drh_structures_op,
    transform_drh_altids_op,
    transform_drh_details_op,
    format_drh_annotations_asset,
    format_drh_structures_asset,
    format_drh_altids_asset,
    format_drh_details_asset,
)
from src.ops.chembl_ftp_ops import (
    get_chembl_structures_op,
    get_chembl_uniprot_mappings_op,
    format_chembl_structures_asset,
    format_chembl_uniprot_mappings_asset,
)
from src.ops.chembl_api_ops import (
    get_chembl_mechanisms_op,
    get_chembl_activities_op,
    get_chembl_assays_op,
    get_chembl_molecules_op,
    get_chembl_targets_op,
    transform_chembl_mechanisms_op,
    transform_chembl_activities_op,
    transform_chembl_molecules_op,
    transform_chembl_targets_op,
    format_chembl_targets_asset,
    format_chembl_mechanisms_asset,
    format_chembl_activities_asset,
    format_chembl_molecules_asset,
)


default_config = config_from_files([file_relative_path(dunderfile=__file__, relative_path="../config/run_config.yaml")])


@job(config=default_config)
def main() -> None:

    create_output_folders()

    """HGNC annotation data extraction"""
    hgnc_data = get_hgnc_op()
    format_hgnc_asset(
        hgnc=concat_hgnc_additional_op(hgnc_df=hgnc_data, hgnc_additional=get_hgnc_additional_op(hgnc=hgnc_data))
    )

    """DRH annotation data extraction"""
    drh_merged_data = merge_drh_op(drugs=get_drh_drugs_op(), samples=get_drh_samples_op())

    format_drh_annotations_asset(annotations=transform_drh_annotations_op(drugs=drh_merged_data))
    format_drh_structures_asset(structures=transform_drh_structures_op(drugs=drh_merged_data))
    format_drh_altids_asset(altids=transform_drh_altids_op(drugs=drh_merged_data))
    format_drh_details_asset(details=transform_drh_details_op(drugs=drh_merged_data))

    """Chembl API annotation data extraction"""
    transformed_activity = transform_chembl_activities_op(
        activities=get_chembl_activities_op(), assays=get_chembl_assays_op()
    )
    transformed_targets = transform_chembl_targets_op(targets=get_chembl_targets_op())
    transformed_mechanisms = transform_chembl_mechanisms_op(mechanisms=get_chembl_mechanisms_op())
    transformed_molecules = transform_chembl_molecules_op(molecules=get_chembl_molecules_op())

    format_chembl_activities_asset(activities=transformed_activity)
    format_chembl_targets_asset(targets=transformed_targets)
    format_chembl_mechanisms_asset(mechanisms=transformed_mechanisms)
    format_chembl_molecules_asset(molecules=transformed_molecules)

    """Chembl FTP annotation data extraction"""
    format_chembl_structures_asset(chembl_structures=get_chembl_structures_op())
    format_chembl_uniprot_mappings_asset(chembl_uniprot_mappings=get_chembl_uniprot_mappings_op())
