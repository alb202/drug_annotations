from dagster import job, config_from_files, file_relative_path
from src.ops.chembl_api_ops import (
    chembl_mechanisms_asset,
    chembl_assays_asset,
    chembl_activities_asset,
    chembl_targets_asset,
    chembl_molecules_asset,
    chembl_activities_transform_asset,
    chembl_targets_transform_asset,
    chembl_molecules_transform_asset,
    chembl_mechanisms_transform_asset,
    chembl_targets_format_asset,
    chembl_mechanisms_format_asset,
    chembl_activities_format_asset,
    chembl_molecules_format_asset,
)
from src.ops.chembl_ftp_ops import (
    chembl_structures_asset,
    chembl_uniprot_mappings_asset,
    chembl_structure_format_asset,
    chembl_uniprot_mappings_format_asset,
)
from src.ops.drh_ops import (
    drh_drugs_asset,
    drh_samples_asset,
    drh_merged,
    drh_annotations_op,
    drh_structures_op,
    drh_altids_op,
    drh_details_op,
    drh_annotations_format_asset,
    drh_structures_format_asset,
    drh_altids_format_asset,
    drh_details_format_asset,
)

default_config = config_from_files([file_relative_path(dunderfile=__file__, relative_path="config/config.yaml")])


@job(config=default_config)
def main() -> None:

    """Chembl API annotation data extraction"""
    transformed_activity = chembl_activities_transform_asset(
        activities=chembl_activities_asset(), assays=chembl_assays_asset()
    )
    transformed_targets = chembl_targets_transform_asset(targets=chembl_targets_asset())
    transformed_mechanisms = chembl_mechanisms_transform_asset(mechanisms=chembl_mechanisms_asset())
    transformed_molecules = chembl_molecules_transform_asset(molecules=chembl_molecules_asset())

    """Chembl FTP annotation data extraction"""
    chembl_activities_format_asset(activities=transformed_activity)
    chembl_targets_format_asset(targets=transformed_targets)
    chembl_mechanisms_format_asset(mechanisms=transformed_mechanisms)
    chembl_molecules_format_asset(molecules=transformed_molecules)

    chembl_structure_format_asset(chembl_structures=chembl_structures_asset()),
    chembl_uniprot_mappings_format_asset(chembl_uniprot_mappings=chembl_uniprot_mappings_asset())

    """DRH annotation data extraction"""
    drh_merged_data = drh_merged(drugs=drh_drugs_asset(), samples=drh_samples_asset())

    drh_annotations_format_asset(annotations=drh_annotations_op(drugs=drh_merged_data))
    drh_structures_format_asset(structures=drh_structures_op(drugs=drh_merged_data))
    drh_altids_format_asset(altids=drh_altids_op(drugs=drh_merged_data))
    drh_details_format_asset(details=drh_details_op(drugs=drh_merged_data))
