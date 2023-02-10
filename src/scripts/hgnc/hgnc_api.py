from joblib import Parallel, delayed
from itertools import chain
from requests import Session
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from urllib3.util.timeout import Timeout
from tqdm import tqdm

tqdm.pandas()


class HGNC:

    RETRY_STRATEGY = Retry(total=20, backoff_factor=1, status_forcelist=[429, 502, 503, 504], allowed_methods=["GET"])
    TIMEOUT_STRATEGY = Timeout(connect=20, read=20)
    FETCH_URL = "http://rest.genenames.org/fetch/"
    HEADERS = {"Accept": "application/json"}

    def __init__(self, n_jobs: int = 12) -> None:
        """Initializes the HGNC object and create a new session to make requests"""

        self.session = Session()
        self.session.mount("https://", HTTPAdapter(max_retries=self.RETRY_STRATEGY))
        self.n_jobs = n_jobs

    def fetch_id(self, id: int | str) -> list:
        """Get records for a gene id"""
        url = f"{self.FETCH_URL}hgnc_id/{str(id)}"
        return self._call_api(url=url)

    def fetch_symbol(self, symbol: str) -> list:
        """Get records for a symbol"""
        url = f"{self.FETCH_URL}symbol/{str(symbol)}"
        return self._call_api(url=url)

    def fetch_ids(self, id_list: list) -> list:
        return self._run_parallel(lst=id_list, func=self.fetch_id, description="Fetching HGNC ids")

    def fetch_symbols(self, symbol_list: list) -> list:
        return self._run_parallel(lst=symbol_list, func=self.fetch_symbol, description="Fetching HGNC symbols")

    def _call_api(self, url: str) -> list:
        """Call the HGNC api"""
        r = self.session.get(url=url, headers=self.HEADERS)
        if r.status_code == 200:
            return r.json().get("response").get("docs")
        return None

    def _run_parallel(self, lst: list, func: callable, description: str = None) -> list:
        r = Parallel(n_jobs=self.n_jobs)(delayed(func)(item) for item in tqdm(lst, desc=description))
        return list(chain.from_iterable(r))


# a = HGNC()
# # r = a.fetch_ids([384, 2934, 21284])
# r = a.fetch_symbols(["FKBP5"])

# print(r)
