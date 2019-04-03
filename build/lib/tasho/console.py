import datetime

class Console():

    logLevel = 5
    
    @classmethod
    def ts(self):
        return str(datetime.datetime.now())

    @classmethod
    def log(self, *args, **kwargs):
        if self.logLevel <= 0:
            print(f"[{self.ts()}] Info   :", *args, **kwargs)
    
    @classmethod
    def warning(self, *args, **kwargs):
        if self.logLevel <= 1: 
            print(f"[{self.ts()}] Warning:", *args, **kwargs)
    
    @classmethod
    def error(self, *args, **kwargs):
        if self.logLevel <= 2:
            print(f"[{self.ts()}] Error  :", *args, **kwargs)