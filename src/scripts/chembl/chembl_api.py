from requests import Session
from requests.models import Response
from requests.adapters import HTTPAdapter
from urllib3 import disable_warnings
from urllib3.exceptions import InsecureRequestWarning
from urllib3.util import Retry, Timeout

from pandas import DataFrame, concat
from tqdm.auto import tqdm
from joblib import delayed, Parallel

disable_warnings(InsecureRequestWarning)


class InvalidEndpointError(Exception):
    """Raised when an invalid endpoint is provided"""

    def __init__(self, endpoint: str):
        self.endpoint = endpoint
        self.message = f"Invalid ChEMBL endpoint: {endpoint}"
        super().__init__(self.message)


class ChemblAPI:
    """API for accessing ChEMBL data"""

    MAX_JOBS = 16
    RETRY_STRATEGY = Retry(total=100, backoff_factor=1, status_forcelist=[429, 502, 503, 504], allowed_methods=["GET"])
    TIMEOUT_STRATEGY = Timeout(connect=20, read=20)
    ENDPOINTS = {
        "activity": "activities",
        "assay": "assays",
        "atc_class": "atc",
        "binding_site": "binding_sites",
        "biotherapeutic": "biotherapeutics",
        "cell_line": "cell_lines",
        "chembl_id_lookup": "chembl_id_lookups",
        "compound_record": "compound_records",
        "compound_structural_alert": "compound_structural_alerts",
        "document": "documents",
        "document_similarity": "document_similarities",
        "document_term": "document_terms",
        "drug": "drugs",
        "drug_indication": "drug_indications",
        "drug_warning": "drug_warnings",
        "go_slim": "go_slims",
        "mechanism": "mechanisms",
        "metabolism": "metabolisms",
        "molecule": "molecules",
        "molecule_form": "molecule_forms",
        "organism": "organisms",
        "protein_class": "protein_classes",
        "source": "source",
        "target": "targets",
        "target_component": "target_components",
        "target_relation": "target_relations",
        "tissue": "tissues",
        "xref_source": "xref_sources",
    }

    def __init__(self, n_jobs: int = 1) -> None:
        self.url = "https://www.ebi.ac.uk/chembl/api/data/"
        self.n_jobs = n_jobs if isinstance(n_jobs, int) and n_jobs <= self.MAX_JOBS and n_jobs > 0 else 1

        """ Mount session"""
        self.session = Session()
        self.session.mount("https://", HTTPAdapter(max_retries=self.RETRY_STRATEGY))

    def _update_url(self, endpoint: str) -> str:
        return f"{self.url}{endpoint}"

    def _update_limit(self, limit: int, max_records: int) -> int:
        if limit < 0 or limit > 1000:
            limit = 1000
        return min([limit, max_records])

        # return limit if (limit > 0 and limit <= 1000) else 1000

    def _update_max_records(self, max_records: int, endpoint_max: int) -> int:
        if max_records > 0:
            return min([max_records, endpoint_max])
        return endpoint_max

        # max_records = max_records if max_records > 0 else endpoint_max
        # return max_records if max_records <= endpoint_max else endpoint_max

    def _update_n_jobs(self, max_records: int) -> int:
        return self.n_jobs if self.n_jobs <= max_records else max_records

    def _run_parallel(self, n_jobs: int, func, url: str, params: list, description: str = None, **kwags) -> list:
        """Run functions in parallel"""
        return Parallel(n_jobs=n_jobs)(
            delayed(func)({"url": url, "params": {"format": "json", **i, **kwags}})
            for i in tqdm(params, desc=description)
        )

    def _make_request(self, params) -> Response:
        """Make the GET request to the ChEMBL server"""
        return self.session.get(**params, verify=False)

    def _check_endpoint(self, endpoint) -> None:
        if endpoint not in self.ENDPOINTS:
            raise InvalidEndpointError(endpoint=endpoint)

    def retrieve_data(
        self, endpoint: str, max_records: int = -1, offset: int = 0, limit: int = 1000, **kwags
    ) -> DataFrame:
        """Retrieve data from ChEMBL API Endpoint

        Returns:
            DataFrame: A DataFrame containing the retrieved data
        """

        self._check_endpoint(endpoint=endpoint)
        url = self._update_url(endpoint)

        endpoint_max_records = (
            self._make_request({"url": url, "params": {"format": "json", "limit": 1, **kwags}})
            .json()
            .get("page_meta")
            .get("total_count")
        )

        max_records = self._update_max_records(max_records=max_records, endpoint_max=endpoint_max_records)

        limit = self._update_limit(limit, max_records)

        n_jobs = self._update_n_jobs(max_records)

        param_list = [
            {"offset": chunk_start, "limit": limit if limit + chunk_start <= max_records else max_records - chunk_start}
            for chunk_start in (range(offset, max_records, limit))
        ]

        description = f"ChEMBL endpoint: {endpoint}" if endpoint else None

        results = self._run_parallel(
            n_jobs=n_jobs, url=url, func=self._make_request, description=description, params=param_list, **kwags
        )
        endpoint_results_key = self.ENDPOINTS.get(endpoint)
        results_json = [i.json() for i in results]
        results_data = [i.get(endpoint_results_key) for i in results_json]
        results_dataframe_list = [DataFrame(i) for i in results_data]
        df = concat(results_dataframe_list).reset_index(drop=True)
        return df


# a = ChemblAPI(n_jobs=2)
# b = a.retrieve_data(endpoint="target", max_records=50, limit=50)
# print(b)
