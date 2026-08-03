class ActionContext:
    def __init__(self, spark, config, obj):
        self.spark = spark
        self.config = config
        self.obj = obj

    def __enter__(self):
        return self.obj

    def __exit__(self, *args):
        self.obj.action(self.spark, self.config)
        return False
