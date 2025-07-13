import os
import sys
def error_detail_function(error,error_detail:sys):
    try:
        _,_,exc_tb=error_detail.exc_info()

        if exc_tb is None:
            return f"Error occured ;{str(error)}"
        filename=os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        error_message="Error occured python script[{0}]at line number[{1}] error message[{2}]".format(filename,exc_tb.tb_lineno,str(error))
        return error_message
    
    except Exception as e:
        return f"Error occured:{str(error)}.Additionally,error in exception handling:{str(e)}"


class CustomException(Exception):
    def __init__(self,error_message,error_detail=sys):
        super().__init__(error_message)
        self.error_message=error_detail_function(error_message,error_detail)

    def __str__(self):
        return self.error_message



