/*
 * Compilation : g++ -o line_server line_server.cpp -I /usr/local/local64/ACE_wrappers-5.6.1_64b -L /usr/local/local64/ACE_wrappers-5.6.1_64b/lib -lACE
 * Listening Port : 25001
 */

#include <iostream> 
#include <cassert>
#include <map>

#include <ace/INET_Addr.h>
#include <ace/SOCK_Stream.h>
#include <ace/Reactor.h>
#include <ace/Acceptor.h>
#include <ace/Svc_Handler.h>
#include <ace/SOCK_Acceptor.h>
#include <ace/Task.h>
#include <ace/Synch.h>

#include <ace/OS_NS_unistd.h>
#include <ace/OS_main.h>


// define max bytes that will be recieved at a time
#define MAXBUF 5000
#define PORT_TO_LISTEN 60000

#ifndef MAXHOSTNAMELEN
#define MAXHOSTNAMELEN 256
#endif

#define HWM 20971520      // High water mark set to 20MB

#define BUF_EVENT_SIZE 1024

static const int TIMEOUT_IN_SECS = 60;

static bool is_vowel(char c) {
    switch (c) {
    case 'A': case 'a':
    case 'E': case 'e':
    case 'I': case 'i':
    case 'O': case 'o':
    case 'U': case 'u':
        return true;
    default:
        return false;
    };
}

static std::vector<std::string> split_message(std::string& msg)
{
    std::vector<std::string> result;
    size_t first = 0;
    size_t pos = msg.find_first_of(" \n");

    while (pos != std::string::npos) {
        result.push_back(msg.substr(first, pos - first));
        std::cout << result.back() << std::endl;
        first = pos + 1;
        pos = msg.find_first_of(" \n", first);
    }
    result.push_back(msg.substr(first, msg.size() - first));

    return result;
}


template<typename Stream>
class Logger
{
public:
    Logger(Stream& stream):m_mutex(), m_stream(stream){}

    void log(const std::string& msg)
    {
        LockGuard _(m_mutex);
        m_stream << msg << std::endl;
    }
private:
    // Non Copyable and non assignable
    Logger(const Logger&);
    void operator=(const Logger&);

private:
    ACE_Thread_Mutex m_mutex;

    struct LockGuard {
        LockGuard(ACE_Thread_Mutex& m): m_(m) {
            m_.acquire();
        }
        ~LockGuard() { m_.release(); }
        ACE_Thread_Mutex& m_; 
    };

    Stream& m_stream;

};

typedef Logger<std::ostream> ConsoleLogger;// This ought to be a singleton


/*
 * Block processing interface
 */
class IBlock {
public:
    IBlock(): m_next(0)
    {}
    virtual ~IBlock() {}

public:
    // Interface Methods
    virtual void print_input() = 0;
    virtual void start_process(std::vector<std::string>& data) = 0;
public:
    // Fors an intrusive list of Blocks
    void set_next(IBlock* blk) {
        m_next = blk;
    }

    IBlock* next() { return m_next; }

protected:
    IBlock* m_next;
};

// Block processor for pig latin
class Block1 : public IBlock 
{
public:
    Block1(): IBlock() {}
    void print_input() {};
    void start_process(std::vector<std::string>&);
};

// Block processor for soundex
class Block2: public IBlock
{
public:
    Block2() : IBlock() {}
    void print_input() {};
    void start_process(std::vector<std::string>&);
private:
    static char lookup[];
};

// Computes frequency of distinct words
class Block3 : public IBlock
{
public:
    Block3() : IBlock() {}
    void print_input() {};
    void start_process(std::vector<std::string>&);
};

char  Block2::lookup[] = {
        '0',    /* A */
        '1',    /* B */
        '2',    /* C */
        '3',    /* D */
        '0',    /* E */
        '1',    /* F */
        '2',    /* G */
        '0',    /* H */
        '0',    /* I */
        '2',    /* J */
        '2',    /* K */
        '4',    /* L */
        '5',    /* M */
        '5',    /* N */
        '0',    /* O */
        '1',    /* P */
        '0',    /* Q */
        '6',    /* R */
        '2',    /* S */
        '3',    /* T */
        '0',    /* U */
        '1',    /* V */
        '0',    /* W */
        '2',    /* X */
        '0',    /* Y */
        '2',    /* Z */
};

void Block1::start_process(std::vector<std::string>& data_vec)
{
    std::cout << "Block1::start_process" << std::endl;
    for (int i = 0; i < data_vec.size(); i++) {
        if (data_vec[i].empty()) continue;

        if (is_vowel(data_vec[i][0])) {
            data_vec[i].append("yay");
        } else {
            std::string str;
            str.reserve(data_vec[i].size() + 2);
            std::copy(data_vec[i].begin() + 1, data_vec[i].end(), 
                     std::back_inserter(str));
            str.append(1, data_vec[i][0]);
            str.append("ay");
            data_vec[i] = str;
        }
    }
    if (m_next) {
        m_next->start_process(data_vec);
    }
}

void Block2::start_process(std::vector<std::string>& data_vec)
{
    std::cout << "Block2::start_process" << std::endl;
    for (int i = 0; i < data_vec.size(); i++) {
        if (data_vec[i].empty()) continue;

        std::string res;
        res.append(1, data_vec[i][0]); // Keep the first character as it is
        for (int j = 1; j < data_vec[i].size(); j++) {
            if (!is_vowel(data_vec[i][j])) {
                if (std::islower(data_vec[i][j]))
                    res.append(1, Block2::lookup[data_vec[i][j] - 97]);
                else 
                    res.append(1, Block2::lookup[data_vec[i][j] - 65]);
            }
        }
        data_vec[i] = res;
        std::cout << res << std::endl;
    }
    if (m_next) {
        m_next->start_process(data_vec);
    }
}

void Block3::start_process(std::vector<std::string>& data_vec)
{
    std::cout << "Block3::start_process" << std::endl;
    std::map<std::string, int> freq_counter;
    for (int i = 0; i < data_vec.size(); i++) {
        if (data_vec[i].empty()) continue;

        ++freq_counter[data_vec[i]];
    }

    //ATTN: What to do with the count ? print it ??
    if (m_next) {
        m_next->start_process(data_vec);
    }
}

/* @desc: Compute_Task implements the Half sync portion of
 * the Half-Sync/Half-Async Pattern.
 * There are 4 threads configured to read the message from
 * the synchronized ACE_Message_Queue.
 * The message received has both the SOCK_Stream pointer
 * and the actual network message recieved .
 */

class Compute_Task: public ACE_Task<ACE_MT_SYNCH>
{
public:
    static const int HIGH_WATER_MARK = 1024 * 20000;
    static const int MAX_THREADS     = 4;
public:
    Compute_Task(ConsoleLogger& log): log_(log)
    {
        msg_queue()->high_water_mark(HIGH_WATER_MARK);
        this->activate(THR_NEW_LWP, MAX_THREADS);
    }

    virtual int svc();
    virtual int put(ACE_Message_Block* client_input, ACE_Time_Value* timeout = 0)
    {
        return putq(client_input,timeout);
    }

private:
    ConsoleLogger& log_;
    Block1 m_b1;
    Block2 m_b2;
    Block3 m_b3;
    //ATTN: This chain could also be pre-created if at all
    //this function becomes a major performance bottleneck
    //which is extremely unlikely.
    IBlock* get_process_chain(char msg_type)
    {
        switch (msg_type) {
        case 'A':
            m_b1.set_next(&m_b2);
            m_b2.set_next(&m_b3);
            return &m_b1;
            break;
        case 'B':
            m_b2.set_next(&m_b3);
            return &m_b2;
            break;
        case 'C':
            m_b1.set_next(&m_b3);
            return &m_b1;
            break;
        default:
            std::cerr << "Invalid message type obtained: " << msg_type << std::endl;
            return NULL;
        };
    }

};

int Compute_Task::svc()
{
    for(ACE_Message_Block* input; getq(input) != -1;)
    {
        std::cout << "Processing Message" << std::endl;
        std::string message;
        std::cout << "Message len : " << input->total_length() << std::endl;
        message.reserve(input->total_length()); // saves lot of reallocations
        char msg_type = *input->rd_ptr();
        IBlock* pchain = get_process_chain(msg_type);

        ACE_Message_Block* in = input;
        in->rd_ptr(1); // Offset the message type
        in->rd_ptr(1); // offset new line
        do {
            message.append(in->rd_ptr(), in->wr_ptr() - in->rd_ptr());
            in = in->cont();
        } while(in);

        std::vector<std::string> all_words = split_message(message);
        pchain->start_process(all_words);

        input->release();
    }

    return 0;
}

// ------------------------ Echo Task Sync Ends here -----------------------------



/* Line_Svc_Handler : Implements Half ASync Layer of the HS/HA pattern
 * This class puts the received messages from network to the ACE_Task 
 * Message Queue.
 * Reordering of the messages not handled using suspend and resume handler
 * because of cyclic dependency issue and cannot be resolved by using header file
 * in this course.
 */

 
class Line_Svc_Handler: public ACE_Svc_Handler<ACE_SOCK_Stream, ACE_NULL_SYNCH>
{
public:
    typedef ACE_Svc_Handler<ACE_SOCK_Stream, ACE_NULL_SYNCH> PARENT;

public:
    Line_Svc_Handler():PARENT(){}
    Line_Svc_Handler(Compute_Task* task): m_task(task),
                                       m_msglen(-1),
                                       m_counting_bytes(0),
                                       m_client_data(0),
                                       m_curr_read_blk(0)
    {
        ACE_OS::memset(peer_name, 0, sizeof(peer_name));
    }

public:
    int open(void*); 

    int handle_timeout(ACE_Time_Value const&, void const*)
    {
        std::cout << "Removing Handler for client : " << peer_name << " due to timeout" << std::endl;
        this->reactor()->remove_handler(this, ACE_Event_Handler::READ_MASK);
        return 0;
    }
    
    int handle_input(ACE_HANDLE /* handle */);
    int handle_close () { PARENT::handle_close(); delete this; return 0;}

public:
    int suspend_handler();
    int resume_handler();

protected:
    int recv_from_network();   
    void delegate_to_task();
    int reschedule_timer();

    
private:
    char peer_name[MAXHOSTNAMELEN];
    Compute_Task* m_task;

    // Total Message length
    int m_msglen;
    // Keeps track of bytes read in a single message
    int m_counting_bytes; 
    // Data buffer where message is copied form the network
    ACE_Message_Block* m_client_data;
    ACE_Message_Block* m_curr_read_blk;
};

int Line_Svc_Handler::open(void* acceptor)
{
    msg_queue()->high_water_mark(HWM) ;
    
    if(reactor()->schedule_timer(this, 0, ACE_Time_Value(TIMEOUT_IN_SECS)) == -1)
    {
        std::cout << "Error while scheduling timer" << std::endl;
        return -1;
    }
    else if(this->peer().enable(ACE_NONBLOCK) == -1)
    {
        std::cout << "Error while setting the handle as non-blocking" << std::endl;
        return -1;
    }
    else if(PARENT::open(acceptor) == -1)
    {
        std::cout << "Error while accepting connection" << std::endl;
        return -1;
    }
    else
    {
        ACE_INET_Addr peer_addr;

        if(this->peer().get_remote_addr(peer_addr) == 0
            &&
            peer_addr.addr_to_string(peer_name, MAXHOSTNAMELEN) == 0)
        {
            std::cout << "Connection received from : " << peer_name << std::endl;
        }
    }
    return 0;
}

int Line_Svc_Handler::handle_input(ACE_HANDLE handle)
{
    // Reschedule the timer
    if(reschedule_timer() == -1) {
        std::cout << "reschedule_timer FAILED!!" << std::endl;
        return -1;
    }

    int bytes_read = recv_from_network();
    std::cout << "Bytes read = " << bytes_read << std::endl;
    switch (bytes_read) {
    case -1:
        return -1;
    case 0:
        std::cout << "Client channel closed" << std::endl;
        return -1;
    default:
    {
        if (m_msglen == -1) {
            // First read of new message
            std::cout << "Got new message" << std::endl;
            if (bytes_read >= 4) {
                m_msglen = *reinterpret_cast<int*>(m_client_data->rd_ptr());
                std::cout << "fdf: " << m_msglen << std::endl;
                m_msglen = ACE_NTOHL (m_msglen) + sizeof (m_msglen);
                std::cout << "Got message len = " << m_msglen << std::endl;
                m_client_data->rd_ptr(sizeof (m_msglen));
            }
        }
        if (bytes_read + m_counting_bytes == m_msglen) {
            std::cout << "Got all message till now: " << m_client_data->length() << std::endl;
            this->delegate_to_task();
            return -1; // Tell ACE to remove this from reactor
        }
        m_counting_bytes += bytes_read;
        return 0;
    }
    };
}


int Line_Svc_Handler::recv_from_network()
{
    /*
     * Structure of a message
     * 4 BYTES -> Length of Message
     * 1 BYTE  -> Message type
     * '\n'
     * Multi character line + '\n'
     * REPEAT
     */
    ACE_Message_Block* read_blk;

    if (m_client_data == NULL) {
        ACE_NEW_RETURN (m_client_data, ACE_Message_Block(BUF_EVENT_SIZE), -1);
        read_blk = m_client_data;
    } else {
        ACE_NEW_RETURN (read_blk, ACE_Message_Block(BUF_EVENT_SIZE), -1);
        m_curr_read_blk->cont(read_blk);
    }
    m_curr_read_blk = read_blk;

    ssize_t br = this->peer().recv(m_curr_read_blk->wr_ptr(), BUF_EVENT_SIZE);

    // Set the write pointer of the message
    m_curr_read_blk->wr_ptr(br);

    return br;
}

void Line_Svc_Handler::delegate_to_task()
{
    std::cout << "Try dispatching it to worker thread pool: " << 
        m_client_data->total_length() << std::endl;
    this->m_task->put(m_client_data);
}

int Line_Svc_Handler::suspend_handler()
{
    return this->reactor()->suspend_handler(this); 
}

int 
Line_Svc_Handler::resume_handler()
{
    return this->reactor()->resume_handler(this);
}


int Line_Svc_Handler::reschedule_timer()
{
    // cancel the current running timer
    if(this->reactor()->cancel_timer(this) == -1)
    {
        std::cout << "Error while cancelling timer" << std::endl;
        return -1;
    }
    else if(this->reactor()->schedule_timer(this, 0, ACE_Time_Value(TIMEOUT_IN_SECS)) == -1)
    {
        std::cout << "Error while scheduling timer" << std::endl;
        return -1;
    }
    else
        return 0;
}



// --------------------- Half ASync Layer Ends Here ---------------------------------


/*
 * Acceptor part of the Acceptor-Connector framework 
 *
 */

class Line_Acceptor: public ACE_Acceptor<Line_Svc_Handler, ACE_SOCK_Acceptor>
{
public:
    typedef ACE_Acceptor<Line_Svc_Handler, ACE_SOCK_Acceptor> PARENT;
    template<typename T>
    Line_Acceptor(Logger<T>& log):m_task(log) {}

public:
    virtual int open(ACE_INET_Addr /*addr*/, ACE_Reactor* /*reactor*/);
    virtual int make_svc_handler(Line_Svc_Handler *& /*sh*/);

private:
    Compute_Task m_task;
};


int Line_Acceptor::open(ACE_INET_Addr addr, ACE_Reactor* reactor)
{
    std::cout << "Acceptor has started..." << std::endl;
    return PARENT::open(addr, reactor);
}

int Line_Acceptor::make_svc_handler(Line_Svc_Handler*& svc_handler)
{
    if(svc_handler == 0)
    {
        std::cout << "Create new svc handler" << std::endl;
        ACE_NEW_RETURN(svc_handler, Line_Svc_Handler(&m_task), -1);
        svc_handler->reactor(this->reactor());
    }
    return 0;
}

// ------------------- Acceptor part ends here ------------------------------


int main()
{
    ConsoleLogger logger(std::cout);
    ACE_Reactor reactor;
    Line_Acceptor Line_Acceptor(logger); 

    if (Line_Acceptor.open (ACE_INET_Addr(25001),
                          &reactor) == -1)
        return 1;

    reactor.run_reactor_event_loop ();
    return 0;
}
