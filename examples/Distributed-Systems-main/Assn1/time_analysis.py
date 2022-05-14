import time
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


def broker_analysis() -> None:
    """ Function process all the time data and display 
    it in a graph

    Arguements:
        None

    Returns:
        None
    """
    
    subs = []

    pub = load_pub_times('output_files/pubproxy.out')
    subs.append(load_sub_times('output_files/subproxy_1.out'))
    subs.append(load_sub_times('output_files/subproxy_2.out'))

    time_diff = {}

    for sub in subs:
        for ip in sub:
            for topic in sub[ip]:

                sub_times = sub[ip][topic]
                pub_times = pub[ip][topic][:len(sub_times)]
                time_diff[topic] = []

                for i in range(len(sub_times)):
                
                    start = float(pub_times[i])
                    finish = float(sub_times[i])
                    time_diff[topic].append((finish-start))
                
                plt.plot([i for i in range(len(time_diff[topic]))], time_diff[topic], label=topic)
                plt.title('Message Times')
                plt.xlabel('Message Number')
                plt.ylabel('Time taken (ms)')


    plt.legend(loc='best')
    plt.show()

def direct_analysis() -> None:
    """ Function process all the time data and display 
    it in a graph

    Arguements:
        None

    Returns:
        None
    """

    pubs = []

    sub1_times = load_sub_times('output_files/subproxy_1.out')
    pubs.append(load_pub_times('output_files/pubproxy_1.out'))
    pubs.append(load_pub_times('output_files/pubproxy_2.out'))
    pubs.append(load_pub_times('output_files/pubproxy_3.out'))

    pubs_dict = {}

    for pub in pubs:
        for ip in pub:

            pubs_dict[ip] = {}
            for topic in pub[ip]:
                pubs_dict[ip][topic] = pub[ip][topic]
    
    time_diff = {}

    for ip in sub1_times:
        for topic in sub1_times[ip]:

            sub_times = sub1_times[ip][topic]
            pub_times = pubs_dict[ip][topic][:len(sub_times)]
            time_diff[ip] = []

            for i in range(len(sub_times)):
                
                start = float(pub_times[i])
                finish = float(sub_times[i])
                time_diff[ip].append((finish-start))
            
            plt.plot([i for i in range(len(time_diff[ip]))], time_diff[ip], label=ip)
            plt.title('Message Times')
            plt.xlabel('Message Number')
            plt.ylabel('Time taken (ms)')
    
    plt.legend(loc='best')
    plt.show()


    
def main():
    
    if sys.argv[1] == 'broker':
        broker_analysis()
    
    elif sys.argv[1] == 'direct':
        direct_analysis()

if __name__ == '__main__':
    main()