from time import sleep
import sys

from topiclist import TopicList
from pubproxy import PublisherProxy

def broker_test() -> None:
    """ Creates new publisher proxy and begins to publish
    random topics

    Arguements:
        None

    Returns:
        None
    """

    sleep(5)

    z_ip = sys.argv[2]
    z_port = sys.argv[3]

    topic_list = TopicList()
    topics = topic_list.interest()

    if 'pressure' not in topics:
        topics.append('pressure')
    
    if 'humidity' not in topics:
        topics.append('humidity')

    p = PublisherProxy("Dissemination", z_ip, z_port, topics)

    while(True):
        print('pub app')
        p.publish_rand_topic(p.pub.ip)
        sleep(2)

def direct_test() -> None:
    """ Creates new publisher proxy and begins to publish
    random topics

    Arguements:
        None

    Returns:
        None
    """

    z_ip = sys.argv[2]
    z_port = sys.argv[3]
    pub_port = sys.argv[4]

    topic_list = TopicList()
    topics = topic_list.interest()

    if 'pressure' not in topics:
        topics[0] = 'pressure'

    p = PublisherProxy("Direct", z_ip, z_port, topics, pub_port)

    while(True):

        p.publish_rand_topic(p.pub.ip)
        sleep(2)


def main():

    if sys.argv[1] == "broker":
        broker_test()

    elif sys.argv[1] == "direct":
        direct_test()
    

if __name__ == '__main__':
    main()
