##########################################################################
## CS450/550 Assignment 1 - Streaming Algorithms
##
## Template code for assignment 1
## Do not edit anywhere except blocks where a #[TODO]# appears
##
## Student Name: Katya Griffiths-Julien, Jerry Lau
## Student ID: 002282220, 002310931

import sys
from pprint import pprint
from random import random
from collections import deque
from sys import getsizeof
import resource
import numpy as np

##########################################################################
##########################################################################
# Methods: implement the methods of the assignment below.  
#
# Each method gets 1 100 element array for holding ints of floats. 
# This array is called memory1a, memory1b, or memory1c
# You may not store anything else outside the scope of the method.
# "current memory size" printed by main should not exceed 8,000.

MEMORY_SIZE = 100 #do not edit

memory1a =  deque([None] * MEMORY_SIZE, maxlen=MEMORY_SIZE) #do not edit

def task1ADistinctValues(element, returnResult = True):
    #[TODO]#
    #procss the element you may only use memory1a, storing at most 100 

    #global memory1a
    
    val = trailing_zeros(element)

    temp_max = memory1a.pop()
    if temp_max is None:
      temp_max = 0

    if val > temp_max:
      temp_max = val

    memory1a.append(temp_max)

    temp_inc = memory1a.popleft()
    if temp_inc is None:
      temp_inc = 0
    temp_inc+=1
    memory1a.appendleft(temp_inc)
    
    if returnResult: #when the stream is requesting the current result
        result = 0
        #[TODO]#
        #any additional processing to return the result at this point
        
        max_num = memory1a.pop()
        memory1a.append(max_num)
        inc = memory1a.popleft()
        memory1a.appendleft(inc)

        result = 2 ** max_num

        if result > inc:
            result = inc        
        
        return result
    else: #no need to return a result
        pass

def trailing_zeros(x):
    """ Counting number of trailing zeros
    in the binary representation of x."""
    if x == 0:
        return 1
    count = 0
    while x & 1 == 0:
        count += 1
        x >>= 1
    return count

memory1b =  deque([None] * MEMORY_SIZE, maxlen=MEMORY_SIZE) #do not edit

def task1BMedian(element, returnResult = True):
    #[TODO]#
    #procss the element
    
    global memory1b

    if element > len(memory1b) - 1:
      element = len(memory1b) - 1

    val = memory1b[element]

    if val is None:
      val = 0

    val+=1

    del memory1b[element]
    memory1b.insert(element, val)
    
    if returnResult: #when the stream is requesting the current result
        result = 0
        #[TODO]#
        #any additional processing to return the result at this point
        
        memory1b = [0 if x is None else x for x in memory1b]

        total = np.cumsum(memory1b)[-1]

        myMed = 0
        myInd = None
        for i in memory1b:
          myMed += i
          if myMed >= total / 2:
            myInd = memory1b.index(i)
            break

        if myMed <= total / 2:
          result = myMed
        else:
          result = myInd        
        
        return result
    else: #no need to return a result
        pass
    

memory1c =  deque([None] * MEMORY_SIZE, maxlen=MEMORY_SIZE) #do not edit

def task1CMostFreqValue(element, returnResult = True):
    #[TODO]#
    #procss the element
    
    #global memory1c
    
    if element > len(memory1c) - 1:
      element = len(memory1c) - 1

    val = memory1c[element]

    if val is None:
      val = 0

    val+=1

    del memory1c[element]
    memory1c.insert(element, val)    
    
    if returnResult: #when the stream is requesting the current result
        result = 0
        #[TODO]#
        #any additional processing to return the result at this point
        
        max_val = memory1c.index(max(x for x in memory1c if x is not None))
        result = max_val
        
        return result
    else: #no need to return a result
        pass


##########################################################################
##########################################################################
# MAIN: the code below setups up the stream and calls your methods
# Printouts of the results returned will be done every so often
# DO NOT EDIT BELOW

def getMemorySize(l): #returns sum of all element sizes
    return sum([getsizeof(e) for e in l])+getsizeof(l)

if __name__ == "__main__": #[Uncomment peices to test]
    
    print("\n\nTESTING YOUR CODE\n")
    
    ###################
    ## The main stream loop: 
    print("\n\n*************************\n Beginning stream input \n*************************\n")
    filename = sys.argv[1]#the data file to read into a stream
    printLines = frozenset([10**i for i in range(1, 20)]) #stores lines to print
    peakMem = 0 #tracks peak memory usage
    
    with open(filename, 'r') as infile:
        i = 0#keeps track of lines read
        for line in infile:
        
            #remove \n and convert to int
            element = int(line.strip())
            i += 1
            
            #call tasks         
            if i in printLines: #print status at this point: 
                result1a = task1ADistinctValues(element, returnResult=True)
                result1b = task1BMedian(element, returnResult=True)
                result1c = task1CMostFreqValue(element, returnResult=True)
                
                print(" Result at stream element # %d:" % i)
                print("   1A:     Distinct values: %d" % int(result1a))
                print("   1B:              Median: %.2f" % float(result1b))
                print("   1C: Most frequent value: %d" % int(result1c))
                print(" [current memory sizes: A: %d, B: %d, C: %d]\n" % \
                    (getMemorySize(memory1a), getMemorySize(memory1b), getMemorySize(memory1c)))
                
            else: #just pass for stream processing
                result1a = task1ADistinctValues(element, False)
                result1b = task1BMedian(element, False)
                result1c = task1CMostFreqValue(element, False)
                
            memUsage = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
            if memUsage > peakMem: peakMem = memUsage
        
    print("\n*******************************\n       Stream Terminated \n*******************************")
    print("(peak memory usage was: ", peakMem, ")")