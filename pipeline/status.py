class Status(object):
    '''Object to represent row in status table

    Attributes:
        conn: database connection, usually sqlite3 connection object
        name: name of pipeline job running
        display_name: pretty formatted display name for pipeline
        last_ran: UNIX timestamp (number) for last complete run
        start_time: UNIX timestamp (number) for last complete run start
        status: string representing status/errors with the pipeline
        num_lines: if successful, number of lines processed
        input_checksum: a checksum of the input's contents
    '''
    def __init__(
        self, conn, name, display_name, last_ran, start_time,
        status, frequency, num_lines, input_checksum
    ):
        self.conn = conn
        self.name = name
        self.display_name = display_name
        self.last_ran = last_ran
        self.start_time = start_time
        self.status = status
        self.num_lines = num_lines
        self.input_checksum = input_checksum

    def update(self, **kwargs):
        '''Update the Status object with passed kwargs and write the result
        '''
        for k, v in kwargs.items():
            setattr(self, k, v)
        self.write()

    def write(self):
        '''Insert or replace a status row
        '''
        cur = self.conn.cursor()
        cur.execute(
            '''
            INSERT OR REPLACE INTO status (
                name, display_name, last_ran, start_time,
                input_checksum, status, num_lines
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', (
                self.name, self.display_name, self.last_ran,
                self.start_time, self.input_checksum,
                self.status, self.num_lines
            )
        )
        self.conn.commit()
