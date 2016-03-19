#include <iostream>
#include <chrono>
#include <thread>
#include <atomic>
#include <string>
#include <vector>
#include <map>
#include <deque>
#include <mutex>
#include <condition_variable>
#include <cstring>
extern "C" {
    #include <sys/epoll.h>
    #include <netdb.h>
    #include <netinet/in.h>
    #include <unistd.h>
    #include <sys/socket.h>
    #include <sys/types.h>
    #include <fcntl.h>
    #include <netinet/tcp.h>
}


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


class Compute_Task
{
public:
    static const int NUM_WORKER_THREADS = 4;
public:
    Compute_Task() {}

    void run()
    {
      worker_threads_.reserve(NUM_WORKER_THREADS);
      for (int i = 0; i < NUM_WORKER_THREADS; i++) {
          worker_threads_.push_back(std::thread(&Compute_Task::svc, this));
      }
    }

    ~Compute_Task()
    {
      for (auto& thr : worker_threads_) thr.join();
    }

    // Multiple threads will be running this function
    void svc();
public:
    static void putq(std::vector<uint8_t> msg)
    {
        std::unique_lock<std::mutex> _(q_lock_);
        std::cout << "putq: " << msg.size() << std::endl;
        msg_q_.emplace_back(std::move(msg));
        q_cond_.notify_all();
    }

    static std::vector<uint8_t> getq()
    {
      std::unique_lock<std::mutex> ul(q_lock_);
      std::cout << "trygetq" << std::endl;
      while (msg_q_.empty()) {
        q_cond_.wait(ul); 
      }
      auto msg = std::move(msg_q_.front());
      std::cout << "getq: " << msg.size() << std::endl;
      msg_q_.pop_front();
      return msg;
    }

private:
    // TODO: Apply backpressure to the calling thread
    // Queue of network buffer
    static std::deque<std::vector<uint8_t>> msg_q_;
    static std::mutex q_lock_;
    static std::condition_variable q_cond_;
    static std::vector<std::thread> worker_threads_;

private:
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

std::deque<std::vector<uint8_t>> Compute_Task::msg_q_;
std::mutex Compute_Task::q_lock_;
std::condition_variable Compute_Task::q_cond_;
std::vector<std::thread> Compute_Task::worker_threads_;

void Compute_Task::svc()
{
    std::cout << "Compute_Task::svc" << std::endl;
    while (true) {
        auto msg = getq();
        std::cout << "Processing Message" << std::endl;
        //ATTN / TODO: This 4 must come from below 
        // hard coding due to dependency issue in single file
        std::string message((char*)msg.data() + 4, msg.size() - 4);
        std::cout << "Message len : " << msg.size() << std::endl;
        char msg_type = message[0];
        IBlock* pchain = get_process_chain(msg_type);

        std::vector<std::string> all_words = split_message(message);
        pchain->start_process(all_words);
    }
}

namespace PollEvent {
 
    /// Enumeration for poll interest constants.
    enum Flags {
        /// Data available to read
        READ   = 0x01,
        /// Urgent data available to read
        PRI    = 0x02,
        /// Writing can be performed without blocking
        WRITE  = 0x04,
        /// %Error condition
        ERROR  = 0x08,
        /// Hang up
        HUP    = 0x10,
        REMOVE = 0x1000,
        /// Stream socket peer closed connection
        RDHUP  = 0x2000
    };
};

//--------------- Event Handling Code -------------
class Reactor;

class Reactor
{
public:
    Reactor(int);
    Reactor(const Reactor&) = delete;
    void operator=(const Reactor&) = delete;
public:
    /// The event loop
    void run();
    int epoll_fd() const noexcept;
    int get_reactor_id();
private:
    int epfd_ = -1;
    int reactor_id_ = -1;
};

class ReactorMgr
{
public:
    static const int NUM_REACTOR_THREADS = 4;

    static ReactorMgr* instance();
    static Reactor* get_reactor();
public:
    ReactorMgr();
    void start_reactors();
private:
    static std::mutex mutex_;
    static ReactorMgr* instance_;
    static std::vector<std::unique_ptr<Reactor>> reactors_;
    static std::vector<std::thread> thread_pool_;
    static std::atomic<int> next_reactor_;
};

std::mutex ReactorMgr::mutex_;
ReactorMgr* ReactorMgr::instance_ = nullptr;
std::vector<std::unique_ptr<Reactor>> ReactorMgr::reactors_;
std::vector<std::thread> ReactorMgr::thread_pool_;
std::atomic<int> ReactorMgr::next_reactor_;

/*
 * Base class for all event handling strategies
 */
class EventHandler
{
public:
    EventHandler(int fd): sd_(fd)
    {
        reactor_ = ReactorMgr::get_reactor();
    }
    // called by reactor when it receives
    // network data
    virtual bool handle_event(struct epoll_event* event) = 0;
    virtual bool handle_disconnect();
    bool prepare_socket(int mode = PollEvent::READ);
protected:
    // The socket descriptor
    int sd_ = -1;

    Reactor* reactor_ = nullptr;
};

bool EventHandler::handle_disconnect()
{
    struct epoll_event event;
    if (epoll_ctl(reactor_->epoll_fd(), EPOLL_CTL_DEL, sd_, &event) < 0) {
        std::cerr << "epoll_ctl delete failed: " << std::strerror(errno) << std::endl;
        return false;
    }
    return true;
}

bool EventHandler::prepare_socket(int mode)
{
    struct epoll_event event;
    memset(&event, 0, sizeof(struct epoll_event));
    event.data.ptr = this;
    if (mode & PollEvent::READ)
        event.events |= EPOLLIN;
    if (mode & PollEvent::WRITE)
        event.events |= EPOLLOUT;
    // Make it edge triggered
    event.events |= EPOLLET;

    // Make the socket non-blocking
    int flags = fcntl(sd_, F_GETFL, 0);
    if (flags < 0) return false;
    flags |= O_NONBLOCK;
    //TODO: should check for errors
    fcntl(sd_, F_SETFL, flags);

    // Turn off Nagle
    int one = 1;
    if (setsockopt(sd_, SOL_TCP, TCP_NODELAY, &one, sizeof(one)) < 0) {
        std::cerr << "sockopt for nagle failed: " << std::strerror(errno) << std::endl;
    }

    // Set TCP buffer size
    constexpr int bufsize = 4 * 32768;
    if (setsockopt(sd_, SOL_SOCKET, SO_SNDBUF, (char *)&bufsize, sizeof(bufsize)) < 0) {
        std::cerr << "sockopt for sndbuf failed: " << std::strerror(errno) << std::endl;
    }
    if (setsockopt(sd_, SOL_SOCKET, SO_RCVBUF, (char *)&bufsize, sizeof(bufsize)) < 0) {
        std::cerr << "sockopt for recvbuf failed: " << std::strerror(errno) << std::endl;
    }

    // Add it to epoll
    if (epoll_ctl(reactor_->epoll_fd(), EPOLL_CTL_ADD, sd_, &event) < 0) {
        std::cerr << "Failed to add socket: " << std::strerror(errno) << std::endl;
        return false;
    }
    return true;
}

// Specialized event handler for data reading
class DataEventHandler: public EventHandler
{
public:
    DataEventHandler(int fd, struct sockaddr_in addr);
    bool handle_event(struct epoll_event* event) override;

private:
    int read_n_bytes(int to_read, int buf_pos);
private:
    //TODO: This should not be here
    // protocol handling must be completely different
    // seperation of concerns
    static const int msg_header_size_ = 4; // bytes
    int total_msg_size_ = 0;
    int msg_size_recvd_ = 0;
    int header_size_recvd_ = msg_header_size_;
    bool header_recvd_ = false;
    std::vector<uint8_t> nw_buffer_;
};

DataEventHandler::DataEventHandler(int fd, struct sockaddr_in addr): 
    EventHandler(fd)
{
    std::cout << "Created event handker for connection" << std::endl;
    nw_buffer_.reserve(msg_header_size_); // first just reserve for header
}

int DataEventHandler::read_n_bytes(int nread, int buf_pos)
{
    int nleft = nread;
    while (nleft > 0) {
        int read_bytes = 
            ::read(sd_, nw_buffer_.data() + buf_pos, nleft);

        if (read_bytes < 0) {
            if (errno == EINTR) {
                read_bytes = 0;
                continue;
            }
            if (errno == EAGAIN) break;
            return -1;
        } else if (read_bytes == 0) {
            std::cout << "Client disconnected\n";
            return 0;
        }
        nleft -= read_bytes; 
        buf_pos += read_bytes;
    }
    return nread - nleft;
}

bool DataEventHandler::handle_event(struct epoll_event* event)
{
    if (event->events & EPOLLIN) {
      while (true) {
        if (!header_recvd_) {
            int read_bytes = read_n_bytes(header_size_recvd_, msg_size_recvd_);
            if (read_bytes == -1 || read_bytes == 0) {
                std::cerr << "Read failure: " << std::strerror(errno) << std::endl;
                handle_disconnect();
                return false;
            }
            msg_size_recvd_ += read_bytes;

            if (read_bytes >= msg_header_size_) {
                header_recvd_ = true;
                total_msg_size_ = ntohl(*reinterpret_cast<int*>(nw_buffer_.data())) + msg_header_size_;
                std::cout << "Message size = " << total_msg_size_ << std::endl;
                // Reserve space in nw_buffer_
                nw_buffer_.resize(total_msg_size_); // TODO: Allocation error ?
            } else {
                header_size_recvd_ -= read_bytes;
            }
        } else {
            int read_bytes = read_n_bytes(total_msg_size_ - msg_header_size_, msg_size_recvd_);
            if (read_bytes == -1 || read_bytes == 0) {
                std::cerr << "Read failure: " << std::strerror(errno) << std::endl;
                handle_disconnect();
                return false;
            }
            std::cout << "msg_size_recvd_ = " << msg_size_recvd_ << std::endl;
            msg_size_recvd_ += read_bytes;
            if (msg_size_recvd_ == (total_msg_size_)) {
                std::cout << "Complete message received: "<< nw_buffer_.size() << std::endl;
                Compute_Task::putq(std::move(nw_buffer_));
                break; 
            }
        }
      }
    }
    return true;
}



// Specialized event handler for acceptor
class AcceptEventHandler: public EventHandler
{
public:
    AcceptEventHandler(int fd);
    bool handle_event(struct epoll_event* event) override;
};

AcceptEventHandler::AcceptEventHandler(int fd): EventHandler(fd)
{
    std::cout << "Listen socket: " << fd << std::endl;
}

bool AcceptEventHandler::handle_event(struct epoll_event* event)
{
    struct sockaddr_in addr{};
    socklen_t addr_len = sizeof(sockaddr_in);
    int one = 1;
#if 0
    while (true) {
#endif
    int cli_sd = accept(sd_, (struct sockaddr*)&addr, &addr_len);
    if (cli_sd == -1) {
        std::cerr << "Accept failure: " << std::strerror(errno) << std::endl;
        return false;
    }
    std::cout << "Accepted connection from client: " << cli_sd << std::endl;

    auto handler = new DataEventHandler(cli_sd, addr);

    if (!handler->prepare_socket()) {
        std::cerr << "prepare_socket failed\n";
        return false;
    }

#if 0
    }
#endif
    return true;
}


//---------------- Reactor Code --------------------
Reactor::Reactor(int id): reactor_id_(id)
{
    struct epoll_event event;
    memset(&event, 0, sizeof(struct epoll_event));
    epfd_ = epoll_create(256);
    if (epfd_ == -1) {
        std::cerr << "epoll_create failed\n";
        exit (EXIT_FAILURE);
    }
}

int Reactor::epoll_fd() const noexcept
{
    return epfd_;
}

void Reactor::run()
{
    std::cout << "Running reactor: " << reactor_id_ << std::endl;
    struct epoll_event events[256];

    while (true) {
        int nfds = epoll_wait(epfd_, events, 256, -1);
        if (nfds == -1) {
            std::cerr << "epoll_wait failed: " << std::strerror(errno) << std::endl;;
            exit (EXIT_FAILURE);
        }
        for (int i = 0; i < nfds; i++) {
            auto handler = static_cast<EventHandler*>(events[i].data.ptr);
            if (!handler) continue;
            handler->handle_event(&events[i]);
        }
    }
}

ReactorMgr* ReactorMgr::instance()
{
    std::lock_guard<std::mutex> _(mutex_);
    if (instance_) return instance_;
    return new ReactorMgr();
}

ReactorMgr::ReactorMgr() {
    next_reactor_.store(0);
    reactors_.reserve(NUM_REACTOR_THREADS);
    for (int i = 0; i < NUM_REACTOR_THREADS; i++) {
        reactors_.emplace_back(new Reactor(i));
    }
}

Reactor* ReactorMgr::get_reactor()
{
    return reactors_[next_reactor_++ % NUM_REACTOR_THREADS].get();
}

void ReactorMgr::start_reactors()
{
  thread_pool_.reserve(reactors_.size());

  for (auto& reactor : reactors_) {
    thread_pool_.push_back(std::thread(&Reactor::run, reactor.get()));
  }

  for (auto& thr : thread_pool_) thr.join();
}

int main() {
    // setup reactor
    ReactorMgr::instance();

    // start the worker threads
    Compute_Task task;
    task.run();
    // Setup listening socket
    int sd = -1;
    int one = 1;
    if ((sd = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP)) < 0) {
        std::cerr << "listen sd failed: " << std::strerror(errno) << std::endl;
        exit (EXIT_FAILURE);
    }
    // Make the socket non-blocking
    int flags = fcntl(sd, F_GETFL, 0);
    flags |= O_NONBLOCK;
    //TODO: should check for errors
    fcntl(sd, F_SETFL, flags);

    if (setsockopt(sd, SOL_TCP, TCP_NODELAY, &one, sizeof(one)) < 0) {
        std::cerr << "setsockopt nagle failed: " << std::strerror(errno) << std::endl;
        exit (EXIT_FAILURE);
    }

    if (setsockopt(sd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one)) < 0) {
        std::cerr << "setsockopt reuseaddr failed: " << std::strerror(errno) << std::endl;
        exit (EXIT_FAILURE);
    }

    struct sockaddr_in serv_addr;
    bzero((char *) &serv_addr, sizeof(serv_addr));
    short portno = 25000;
    serv_addr.sin_family = AF_INET;
    serv_addr.sin_addr.s_addr = INADDR_ANY;
    serv_addr.sin_port = htons(portno);

    if (::bind(sd, (const sockaddr *)&serv_addr, sizeof(serv_addr)) < 0) {
        std::cerr << "bind failed: " << std::strerror(errno) << std::endl;
        //TODO: Ideally there should be multiple tries for bind
        exit (EXIT_FAILURE);
    }

    if (::listen(sd, 1000) < 0) {
        std::cerr << "listen failed: " << std::strerror(errno) << std::endl;
        exit (EXIT_FAILURE);
    }

    auto handler = new AcceptEventHandler(sd);
    handler->prepare_socket();
    ReactorMgr::instance()->start_reactors();

    return 0;
}
