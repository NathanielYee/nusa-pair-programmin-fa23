import wrds

class Computstat:
    def __init__(self, username):
        self.username = username
        self.conn = wrds.Connection(wrds_username=self.username)

    def __repr__(self):
        return f'{self.username}'

    def __del__(self):
        self.close_connection()
        print('Deleted')

    def close_connection(self):
        self.conn.close()

    def execute(self, query):
        return self.conn.raw_sql(query)

def main():
    curr = Computstat('aidankaneshiro')
    print(curr)
    del curr


if __name__ == '__main__':
    main()