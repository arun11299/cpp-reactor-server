#include <sys/socket.h>
#include <sys/types.h>
#include <netinet/in.h>
#include <netdb.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <unistd.h>
#include <errno.h>
#include <arpa/inet.h> 
#include <iostream>

struct Message {
    int len;
    char* data;
};

char* serialize(const Message* msg)
{
    char* buf = new char[sizeof(msg->len) + msg->len];
    // copy the length
    int nl = htonl (msg->len);
    std::cout << "htonl = " << nl << std::endl;
    char* cp_buf = buf;
    memcpy(buf, &nl, sizeof(nl));
    buf += sizeof(nl);
    memcpy(buf, msg->data, msg->len);
    return cp_buf;
}

int main()
{
    int sockfd = 0, n = 0;
    char recvBuff[1024];
    struct sockaddr_in serv_addr; 

    memset(recvBuff, '0',sizeof(recvBuff));
    if((sockfd = socket(AF_INET, SOCK_STREAM, 0)) < 0)
    {
        printf("\n Error : Could not create socket \n");
        return 1;
    } 

    memset(&serv_addr, '0', sizeof(serv_addr)); 

    serv_addr.sin_family = AF_INET;
    serv_addr.sin_port = htons(25000); 

    if(inet_pton(AF_INET, "0.0.0.0", &serv_addr.sin_addr)<=0)
    {
        printf("\n inet_pton error occured\n");
        return 1;
    } 

    if( connect(sockfd, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0)
    {
       printf("\n Error : Connect Failed \n");
       return 1;
    } 

    Message msg;
    /*
    msg.data = (char*)"C\nThis Since my discussion thread on the efficiency of the in-memory data structure of ZeroMQ with Martin Sustrik, I have been reading up a bit by bit on efficient data structures, primarily from the perspective of memory utilization. Data structures that provide constant lookup time with minimal memory utilization can give a significant performance boost since access to CPU cache is considerably faster than access to RAM. This post is a compendium of a few data structures I came across and salient aspects about them \
        Excerpt: A Judy tree is generally faster than and uses less memory than contemporary forms of trees such as binary (AVL) trees, b-trees, and skip-lists. When used in the Judy Scalable Hashin configuration, Judy is generally faster then a hashing method at all populations. A (CPU) cache-line fill is additional time required to do a read reference from RAM when a word is not found in cache. In today’s computers the time for a cache-line fill is in the range of 50..2000 machine instructions. Therefore a cache-line fill should be avoided when fewer than 50 instructions can do the same job. Judy rarely compromises speed/space performance for simplicity (Judy will never be called simple except at the API). Judy is designed to avoid cache-line fills wherever possible. The Achilles heel of a simple digital tree is very poor memory utilization, especially when the N in N-ary (the degree or fanout of each branch) increases. The Judy tree design was able to solve this problem. In fact a Judy tree is more memory-efficient than almost any other competitive structure (including a simple linked list). \
        HAT-trie – a cache concious trie http://portal.acm.org/citation.cfm?id=1273761 \
        Excerpt: Tries are the fastest tree-based data structures for managing strings in-memory, but are space-intensive. The burst-trie is almost as fast but reduces space by collapsing trie-chains into buckets. This is not however, a cache-conscious approach and can lead to poor performance on current processors. In this paper, we introduce the HAT-trie, a cache-conscious trie-based data structure that is formed by carefully combining existing components. We evaluate performance using several real-world datasets and against other high-performance data structures. We show strong improvements in both time and space; in most cases approaching that of the cache-conscious hash table. Our HAT-trie is shown to be the most efficient trie-based data structure for managing variable-length strings in-memory while maintaining sort order. \
        Burst Trie http://goanna.cs.rmit.edu.au/~jz/fulltext/acmtois02.pdf \
        Excerpt: Many applications depend on efficient management of large sets of distinct strings in memory. We propose a new data structure, the burst trie, that has significant advantages over existing options for such applications: it requires no more memory than a binary tree; it is as fast as a trie; and, while not as fast as a hash table, a burst trie maintains the strings in sorted or near-sorted order. These experiments show that the burst trie is particularly effective for the skewed frequency distributions common in text collections, and dramatically outperforms all other data structures for the task of managing strings while maintaining sort order. \
        Radix trie (aka Patricia trie) http://en.wikipedia.org/wiki/Radix_tree \
        Excerpt: The radix tree is easiest to understand as a space-optimized trie where each node with only one child is merged with its child. Unlike balanced trees, radix trees permit lookup, insertion, and deletion in O(k) time rather than O(log n) \
        Ternary Search Trees http://en.wikipedia.org/wiki/Ternary_search_tree \
        Excerpt: A trie is optimized for speed at the expense of size. The ternary search tree replaces each node of the trie with a modified binary search tree. For sparse tries, this binary tree will be smaller than a trie node. Each binary tree implements a single-character lookup. It has the typical left and right children which are checked if the lookup character is greater or less than the node’s character, respectively. A third child is used if the lookup character is found on that particular node. Unlike the other children, it links to the root of the binary search tree for the next character in the string";

*/
    msg.data = (char*) "A\nThis\nis a\nTest";
    msg.len = strlen(msg.data);
    std::cout << "Actual strlen = " << msg.len << std::endl;

    send(sockfd, (void*)serialize(&msg), strlen(msg.data) + sizeof(msg.len), 0);

    sleep (70);

    return 0;
}
