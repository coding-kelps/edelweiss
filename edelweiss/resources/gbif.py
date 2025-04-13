from dagster import ConfigurableResource
from typing import Any
from pygbif import occurrences, species
from pydantic import Field
import time

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

  def _check_download_request_available(self) -> bool:
    """
      Check if the GBIF API enables a new download requets
      (there is a limit of only 3 download in preparation/running).
    """
    res = occurrences.download_list(user=self.username, pwd=self.password)

    # https://gbif.github.io/parsers/apidocs/org/gbif/api/model/occurrence/Download.Status.html
    in_prepation_downloads = sum(1 for obj in res["results"] if obj["status"] == "PREPARING" or obj["status"] == "RUNNING")

    return in_prepation_downloads < IN_PREPARATION_DOWNLOADS_LIMIT

  def request_download(self, queries: Any) -> str:
    """
      Requests GBIF to generate a dataset of occurrences from a set of queries.
      A wrapper around the [pygbif.occurrences.download()](https://pygbif.readthedocs.io/en/latest/modules/occurrence.html#pygbif.occurrences.download) method.

      Return the generated key of the dataset to download.

      Warning: The dataset is generally not immediately available to be downloaded.
    """

    while not self._check_download_request_available():
      time.sleep(self.download_request_availability_probe_period_min * 60)

    res = occurrences.download(
        queries=queries,
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

    res = occurrences.download_get(
        key=key,
    )

    if res is None:
      raise Exception("GBIF download get failed")

    return res["path"]

  def get_species_info(self, taxonKey: str) -> dict[str, Any]:
    """
      Get a species information from its taxon key.
      A wrapper around the [pygbif.species.name_usage()](https://pygbif.readthedocs.io/en/latest/modules/species.html#pygbif.species.name_usage) method.

      return a dictionnary of information of the corresponding species.
    """
    res = species.name_usage(key=taxonKey)

    if res is None:
      raise Exception("GBIF get species info failed")
    
    return res.get("vernacularName", None)
