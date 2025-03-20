from abc import ABC, abstractmethod

class FileReader(ABC):

    @abstractmethod
    def read_file(self, file_name, effective_date):
        pass
