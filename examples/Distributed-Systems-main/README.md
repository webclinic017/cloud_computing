# Distributed-Systems

ASSIGNMENT 1

  To run the program, make sure you have mininet installed.
  
  Then, start mininet with this command:
  ```
  sudo -E mn --topo=single,5
  ```
  
  Finally either run:
  ```
  source broker_test.txt
  ```
  or
  ```
  source direct_test.txt
  ```
  
  
  To do the time analysis, run:
  ```
  python3 time_analysis.py broker
  ```
  or
  ```
  python3 time_analysis.py direct
  ```
  
  Please keep in mind, to do time analysis for broker, you must first run the broker mininet and then run the time analysis. Same for the direct. You can not run the broker_test and the direct time analysis right after. 
  
  
  
