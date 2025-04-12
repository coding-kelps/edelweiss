from dagster import ConfigurableResource
from typing import Any
from pygbif import occurrences, species

class GBIFAPIResource(ConfigurableResource):
  """
    A Dagster resource wrapper around the [pygbif](https://github.com/gbif/pygbif)
    integration of the [Global Biodiversity Information Facility](https://gbif.org).
  """

  username: str
  password: str
  email: str

  def request_download(self, queries: Any) -> str:
    """
      Requests GBIF to generate a dataset of occurrences from a set of queries.
      A wrapper around the [pygbif.occurrences.download()](https://pygbif.readthedocs.io/en/latest/modules/occurrence.html#pygbif.occurrences.download) method.

      Return the generated key of the dataset to download.

      Warning: The dataset is generally not immediately available to be downloaded.
    """

    res = occurrences.download(
        queries=queries,
        user=self.username,
        pwd=self.password,
        email=self.email
    )

    if res is None:
      raise Exception("GBIF download request failed")
    
    return res[0]
  
  def get_download_metadata(self, key: str) -> str:
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

  def get_download(self, key: str) -> str:
    """
      Download a GBIF download as a archive file.
      A wrapper around the [pygbif.occurrences.download_get()](https://pygbif.readthedocs.io/en/latest/modules/occurrence.html#pygbif.occurrences.download_get) method.

      Return the path of the downloaded file.
    """

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
