from dagster import ConfigurableResource
from typing import Any
from pygbif import occurrences
from pydantic import Field
import time
import requests
import json
import tempfile

IN_PREPARATION_DOWNLOADS_LIMIT = 3

class GBIFAPIResource(ConfigurableResource):
  """
    A Dagster resource wrapper around the [pygbif](https://github.com/gbif/pygbif)
    integration of the [Global Biodiversity Information Facility](https://gbif.org).
  """

  username: str = Field(description="The username of the GBIF account impersonated by the resource.")
  password: str = Field(description="The password of the GBIF account impersonated by the resource.")
  email: str = Field(description="The email address of the GBIF account impersonated by the resource.")

  download_request_availability_probe_period_min: int = Field(
    default=1,
    description="The duration between checks for GBIF donwload requests in minutes"
  )
  download_availability_probe_period_min: int = Field(
    default=2,
    description="The duration between checks for GBIF donwload availability in minutes"
  )

  def _get_account_downloads(self) -> list[dict[str, Any]]:
    res = occurrences.download_list(user=self.username, pwd=self.password)

    if not res:
      raise Exception("no response")
    
    downloads = res["results"]

    return downloads

  def _check_download_request_available(self) -> bool:
    """
      Check if the GBIF API enables a new download requets
      (there is a limit of only 3 download in preparation/running).
    """

    downloads = self._get_account_downloads()

    # https://gbif.github.io/parsers/apidocs/org/gbif/api/model/occurrence/Download.Status.html
    in_prepation_downloads = sum(1 for obj in downloads if obj["status"] == "PREPARING" or obj["status"] == "RUNNING")

    return in_prepation_downloads < IN_PREPARATION_DOWNLOADS_LIMIT

  def _queries_to_request(self, queries: dict[str, str]) -> dict[str, str]:
    res = requests.get(
      url="https://api.gbif.org/v1/occurrence/download/request/predicate",
      headers={
          "Content-Type": "application/json"
      },
      params={**queries, "format": "SIMPLE_CSV"},
    )

    predicates = json.loads(res.text)

    return predicates

  def _find_similar_download(self, queries: dict[str, str]) -> str | None:
    request = self._queries_to_request(queries=queries)
    downloads = self._get_account_downloads()

    for download in downloads:
      if download["request"]["predicate"] == request["predicate"] \
          and download["request"]["format"] == request["format"] \
          and download["request"]["type"] == request["type"] \
          and (download["status"] == "PREPARING" \
              or download["status"] == "RUNNING" \
              or download["status"] == "SUCCEEDED"):
        return download["key"]
    return None

  def request_download(self, queries: dict[str, str]) -> str:
    """
      Requests GBIF to generate a dataset of occurrences from a set of queries.
      A wrapper around the [pygbif.occurrences.download()](https://pygbif.readthedocs.io/en/latest/modules/occurrence.html#pygbif.occurrences.download) method.

      Return the generated key of the dataset to download.

      Warning: The dataset is generally not immediately available to be downloaded.
    """
    key = self._find_similar_download(queries=queries)
    if key:
      return key
    
    while not self._check_download_request_available():
      time.sleep(self.download_request_availability_probe_period_min * 60)

    formated_queries = [f"{key} = {value}" for key, value in queries.items()]

    res = occurrences.download(
        queries=formated_queries,
        user=self.username,
        pwd=self.password,
        email=self.email
    )

    if res is None:
      raise Exception("GBIF download request failed")
    
    return res[0]
  
  def _get_download_metadata(self, key: str) -> dict[str, str]:
    """
      Get the metadata of a GBIF download.
      A wrapper around the [pygbif.occurrences.download_meta()](https://pygbif.readthedocs.io/en/latest/modules/occurrence.html#pygbif.occurrences.download_meta) method.

      Return metadata of the GBIF download.
    """

    res = occurrences.download_meta(
      key=key,
    )

    if res is None:
      raise Exception("GBIF download get metadata failed")

    return res
  
  def _check_download_availability(self, key: str) -> bool:
    metadata = self._get_download_metadata(key)

    return metadata.get("status") == "SUCCEEDED"
  
  def get_download(self, key: str) -> str:
    """
      Download a GBIF download as a archive file.
      A wrapper around the [pygbif.occurrences.download_get()](https://pygbif.readthedocs.io/en/latest/modules/occurrence.html#pygbif.occurrences.download_get) method.

      Return the path of the downloaded file.
    """

    while not self._check_download_availability(key=key):
      time.sleep(self.download_availability_probe_period_min * 60) 

    tmp_dir = tempfile.TemporaryDirectory(prefix="edelweiss-", suffix=key, delete=False)

    res = occurrences.download_get(
        key=key,
        path=tmp_dir.name
    )

    if res is None:
      raise Exception("GBIF download get failed")

    return res["path"]

  def get_species_vernacular_names(self, taxon_key: str) -> dict[str, str]:
    res = requests.get(
      url=f"https://api.gbif.org/v1/species/{taxon_key}/vernacularNames",
      headers={
          "accept": "application/json"
      },
    )

    data: dict[str, Any] = json.loads(res.text)

    vernaculars = {
      "deu": None,
      "fra": None,
      "eng": None
    }

    for item in data.get("results", []):
      lang = item.get("language")
      if lang in vernaculars and vernaculars[lang] is None:
        vernaculars[lang] = item.get("vernacularName")

    return vernaculars
