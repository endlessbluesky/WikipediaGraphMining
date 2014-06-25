import time

s = 0
t = 0

def start():
    global s
    s = time.clock()

def stop():
    global t
    t = time.clock()
    
def getTimeElapsedInSeconds():
    return (t-s)

def reset():
    s, t=0, 0