from dagster import ConfigurableResource
from pydantic import Field
import psycopg

class PostgreSQLResource(ConfigurableResource):
  """
    A Dagster resource wrapper for interacting with a PostgreSQL database.
  """

  username: str = Field(
    description="The username of the connecting user impersonated by the resource.",
  )
  password: str = Field(
    description="The password of the connecting user impersonated by the resource.",
  )
  host: str = Field(
    default="localhost",
    description="The address of the postgreSQL server.",
  )
  port: str = Field(
    default="5432",
    description="The connecting port of the postgreSQL server.",
  )
  db_name: str = Field(
    description="The name of the database to connect to within the postgreSQL server.",
  )


  def get_connection(self) -> psycopg.Connection:
    return psycopg.connect(
      conninfo=f"postgresql://{self.username}:{self.password}@{self.host}:{self.port}/{self.db_name}"
    )
