import os
import sys
from collections import defaultdict
import matplotlib.pyplot as plt


def load_sub_times(file_name: str) -> dict:
    """ Function to parse the subcriber data from the outfile

    Arguements:
        file_name (str): Name of the file to be parsed

    Returns:
        times (dict): List of times
    """

    times = defaultdict(lambda: defaultdict(list))
    with open(file_name, 'r') as f:

        for line in f:

            if 'Recieved' in line:
                ip = line.split('-')[1].split(' ')[1]
                topic = line.split('-')[1].split(' ')[0]
                t = line.split('-')[2]
                times[ip][topic].append(t.strip('\n'))
    
    return times


def load_pub_times(file_name: str) -> dict:
    """ Function to parse the publisher data from the outfile

    Arguements:
        file_name (str): Name of the file to be parsed

    Returns:
        times (dict): List of times
    """

    times = defaultdict(lambda: defaultdict(list))
    with open(file_name, 'r') as f:

        for line in f:

            if 'Publishing' in line:
                ip = line.split('~')[1].split(' ')[1]
                topic = line.split('~')[1].split(' ')[0]
                t = line.split('~')[2]
                times[ip][topic].append(t.strip('\n'))
    
    return times


def  analysis() -> None:
    """ Function process all the time data and display 
    it in a graph

    Arguements:
        None

    Returns:
        None
    """

    pubs = []
    subs = []

    for f_name in os.listdir('output_files'):

        if 'pubproxy' in f_name:
            pubs.append(load_pub_times(f'output_files/{f_name}'))
        elif 'subproxy' in f_name:
            subs.append(load_sub_times(f'output_files/{f_name}'))

    pubs_dict = {}

    for pub in pubs:
        for ip in pub:

            pubs_dict[ip] = {}
            for topic in pub[ip]:
                pubs_dict[ip][topic] = pub[ip][topic]
    
    time_diff = []

    for i in range(len(subs)):
        for ip in subs[i]:
            print(subs[i][ip])
            
            for topic in subs[i][ip]:

                sub_times = subs[i][ip][topic]
                pub_times = pubs_dict[ip][topic][:len(sub_times)]

                for k in range(len(sub_times)):
                    
                    start = float(pub_times[k])
                    finish = float(sub_times[k])
                    time_diff.append((finish-start))
                
        plt.plot([k for k in range(len(time_diff))], time_diff, label=f'Sub_{i}')
        plt.title('Message Times')
        plt.xlabel('Message Number')
        plt.ylabel('Time taken (s)')
        
    plt.legend(loc='best')
    plt.show()


    
def main():
    
    analysis()

if __name__ == '__main__':
    main()