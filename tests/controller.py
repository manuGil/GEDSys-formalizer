"""
Project: 
Author: Manuel G
Created: 20/02/2018 14:37
License: MIT
"""

import time


class Printer:
    def __init__(self, name):
        self.name = name

    def _print(self, control):
        if control:
            print(self.name, control)


class Controller:

    def __init__(self):
        self.status = 'not ready'
        self.control = False
        self.printer = 'hola'
        while self.control:
            print ('hello')

    def start(self):
        self.control = True

    def stop_printing(self):
        self.control = False


c = Controller()
print('status', c.status)

print('status',  c.status)
c.control= True
print('control value', c.control)


# print('status', c.status)
#
# time.sleep(3)
#
# c.stop_printing()



