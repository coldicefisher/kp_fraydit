from kp_fraydit.producers.producer import Producer

class PriceProducer(Producer):
    def __init__(self, topic_name: str, value_schema_name: str = None, key_schema_name: str = None, 
                    optional_value_args: list = [], optional_key_args: list = [], cache_schemas: bool = True
                ):

        super().__init__(topic_name=topic_name, value_schema_name=value_schema_name, key_schema_name=key_schema_name,
                        optional_value_args=optional_value_args, optional_key_args=optional_key_args,
                        cache_schemas=cache_schemas)