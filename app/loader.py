from database import mssql


class TickerLoader:

    def __init__(self, query):
        self.df = None
        self.query = query
        self.parsed = None

    def fetch(self):
        self.load_table()
        self.parse()
        return self.parsed

    def load_table(self):
        instance = mssql.MSSQLDatabase()
        self.df = instance.select_table(self.query)

    def parse(self):
        self.parsed = list(self.df.to_dict("list").values())[0]
