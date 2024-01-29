class LogEntry:
    def __init__(self, index, term, command):
        self.index = index
        self.term = term
        self.command = command
        
    def to_dict(self):
        return {
            'index': self.index,
            'term': self.term,
            'command': self.command
        }