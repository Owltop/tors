#include <cstddef>
#include <mutex>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <errno.h>
#include <pthread.h>
#include <vector>
#include <iostream>
#include <future>
#include <queue>
#include <algorithm>
#include <fcntl.h>

#include "task.h"


#define DISCOVER_PORT 12345
#define TASK_PORT 12346
#define TIMEOUT 2

struct Server {
    struct sockaddr_in addr;
    bool active;

    bool operator==(const Server& other) const {
        return addr.sin_addr.s_addr == other.addr.sin_addr.s_addr;
    }
};

class ServerList {
public:
    ServerList() = default;

    void addServer(const Server& server) {
        std::lock_guard<std::mutex> lock(mutex);
        
        auto it = std::find_if(servers.begin(), servers.end(),
            [&server](const Server& existing) {
                return existing == server;
            });

        if (it == servers.end()) {
            servers.push_back(server);
            std::cout << "Добавлен новый сервер: " << inet_ntoa(server.addr.sin_addr) 
                      << ":" << ntohs(server.addr.sin_port) << std::endl;
        } else {
            it->active = true;
        }
    }

    void deactivateServer(const Server& server) {
        std::lock_guard<std::mutex> lock(mutex);
        
        auto it = std::find_if(servers.begin(), servers.end(),
            [&server](const Server& existing) {
                return existing == server;
            });

        if (it != servers.end()) {
            it->active = false;
        }
    }

    size_t size() const {
        std::lock_guard<std::mutex> lock(mutex);
        return servers.size();
    }

    Server& operator[](size_t index) {
        std::lock_guard<std::mutex> lock(mutex);
        return servers[index];
    }

    void printActiveServers() const {
        std::lock_guard<std::mutex> lock(mutex);
        std::cout << "\n=== Активные серверы ===" << std::endl;
        for (size_t i = 0; i < servers.size(); ++i) {
            std::cout << i << ": " << inet_ntoa(servers[i].addr.sin_addr)
                      << ":" << ntohs(servers[i].addr.sin_port)
                      << " (статус: " << (servers[i].active ? "активен" : "неактивен") << ")"
                      << std::endl;
        }
        std::cout << "=====================\n" << std::endl;
    }
private:
    std::vector<Server> servers;
    mutable std::mutex mutex;
};

ServerList serverList;

void discoverServers() {
    std::cout << "\nПоиск доступных серверов..." << std::endl;
    
    int sock = socket(AF_INET, SOCK_DGRAM, 0);
    if (sock < 0) {
        throw std::runtime_error("Ошибка создания сокета");
    }

    int broadcast = 1;
    if (setsockopt(sock, SOL_SOCKET, SO_BROADCAST, &broadcast, sizeof(broadcast)) < 0) {
        close(sock);
        throw std::runtime_error("Ошибка установки SO_BROADCAST");
    }

    struct sockaddr_in broadcastAddr;
    broadcastAddr.sin_family = AF_INET;
    broadcastAddr.sin_addr.s_addr = INADDR_BROADCAST;
    broadcastAddr.sin_port = htons(DISCOVER_PORT);

    std::string msg = "DISCOVER";
    if (sendto(sock, msg.c_str(), msg.length(), 0,
               reinterpret_cast<struct sockaddr*>(&broadcastAddr), sizeof(broadcastAddr)) < 0) {
        close(sock);
        throw std::runtime_error("Ошибка отправки broadcast");
    }

    struct timeval tv{TIMEOUT, 0};
    setsockopt(sock, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));

    std::vector<char> buffer(256);
    struct sockaddr_in serverAddr;
    socklen_t addrLen = sizeof(serverAddr);

    while (true) {
        int n = recvfrom(sock, buffer.data(), buffer.size()-1, 0,
                        reinterpret_cast<struct sockaddr*>(&serverAddr), &addrLen);
        if (n < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                break;
            }
            std::cerr << "Ошибка приема ответа: " << strerror(errno) << std::endl;
            continue;
        }

        serverAddr.sin_port = htons(DISCOVER_PORT);
        Server server{serverAddr, true};
        serverList.addServer(server);
    }

    close(sock);
    serverList.printActiveServers();
}

double processTask(Server& server, const Task& task) {
    std::cout << "Отправка задачи на сервер " << inet_ntoa(server.addr.sin_addr)
              << " (диапазон: " << task.start << " - " << task.end 
              << ", шаг: " << task.step << ")" << std::endl;
              
    int sock = socket(AF_INET, SOCK_STREAM, 0);
    if (sock < 0) {
        server.active = false;
        throw std::runtime_error("Ошибка создания сокета");
    }

    struct timeval tv{TIMEOUT, 0};
    setsockopt(sock, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));

    struct sockaddr_in taskAddr = server.addr;
    taskAddr.sin_port = htons(TASK_PORT);

    int flags = fcntl(sock, F_GETFL, 0);
    fcntl(sock, F_SETFL, flags | O_NONBLOCK);

    int conn_result = connect(sock, reinterpret_cast<struct sockaddr*>(&taskAddr), sizeof(taskAddr));

    if (conn_result < 0 && errno != EINPROGRESS) {
        server.active = false;
        close(sock);
        throw std::runtime_error("Ошибка подключения");
    }

    fd_set set;
    FD_ZERO(&set);
    FD_SET(sock, &set);

    conn_result = select(sock + 1, NULL, &set, NULL, &tv);

    if (conn_result <= 0) {
        server.active = false;
        close(sock);
        throw std::runtime_error("Ошибка подключения");
    }

    int so_error;
    socklen_t len = sizeof(so_error);
    getsockopt(sock, SOL_SOCKET, SO_ERROR, &so_error, &len);
    if (so_error != 0) {
        server.active = false;
        close(sock);
        throw std::runtime_error("Ошибка подключения");
    }

    fcntl(sock, F_SETFL, flags);

    if (send(sock, &task, sizeof(Task), 0) < 0) {
        server.active = false;
        close(sock);
        throw std::runtime_error("Ошибка отправки");
    }

    int max_retries = 10;
    int i = 0;

    double result;
    int recv_result;
    while (true) {
        fd_set read_fds;
        FD_ZERO(&read_fds);
        FD_SET(sock, &read_fds);

        // Ждём данных от сервера
        struct timeval wait_time = {TIMEOUT, 0};
        int select_result = select(sock + 1, &read_fds, NULL, NULL, &wait_time);

        if (select_result < 0) {
            server.active = false;
            close(sock);
            throw std::runtime_error("Ошибка ожидания ответа сервера");
        } else if (select_result == 0) {
            // Таймаут ожидания, попробуем ещё раз
            std::cout << "Продолжаем ожидать ответа от сервера "
                      << inet_ntoa(server.addr.sin_addr) << std::endl;
            if (++i == max_retries) {
                throw std::runtime_error("Нет ответа сервера");
            }
            sleep(1);
            continue;
        }

        if (FD_ISSET(sock, &read_fds)) {
            // Данные готовы для чтения
            recv_result = recv(sock, &result, sizeof(result), 0);
            if (recv_result > 0) {
                break; // Успешно получили данные
            } else if (recv_result == 0) {
                server.active = false;
                close(sock);
                throw std::runtime_error("Сервер закрыл соединение");
            } else if (errno != EAGAIN && errno != EWOULDBLOCK) {
                server.active = false;
                close(sock);
                throw std::runtime_error("Ошибка получения");
            }
        }
    }

    std::cout << "Получен результат от сервера " << inet_ntoa(server.addr.sin_addr) 
              << ": " << result << std::endl;

    close(sock);
    return result;
}

double distribute_tasks(const std::vector<Task>& tasks) {
    double total_result = 0.0;
    std::vector<std::pair<size_t, std::future<double>>> futures;
    std::queue<size_t> pending_tasks;
    
    std::cout << "\nРаспределение " << tasks.size() << " задач..." << std::endl;
    
    for (size_t i = 0; i < tasks.size(); ++i) {
        pending_tasks.push(i);
    }

    while (!pending_tasks.empty()) {
        size_t server_count = serverList.size();
        std::vector<Server> taskServers;
        std::cout << "\nОсталось задач: " << pending_tasks.size() << std::endl;
        
        for (size_t i = 0; i < server_count && !pending_tasks.empty(); ++i) {
            Server& server = serverList[i];
            if (!server.active) continue;

            size_t task_index = pending_tasks.front();
            pending_tasks.pop();

            futures.push_back({task_index, std::async(std::launch::async, 
                [&server, task = tasks[task_index]]() {
                    return processTask(server, task);
                }
            )});

            taskServers.push_back(server);
        }

        for (size_t i = 0; i < futures.size(); ++i) {
            auto& futurePair = futures[i];
            auto& task_index = futurePair.first;
            auto& future = futurePair.second;
            try {
                double result = future.get();
                total_result += result;
            } catch (const std::exception& e) {
                std::cerr << "Ошибка выполнения задачи: " << e.what() << std::endl;
                serverList.deactivateServer(taskServers[i]);
                pending_tasks.push(task_index);
                // discoverServers();
            }
        }
        futures.clear();

        if (!pending_tasks.empty()) {
            std::cout << "Поиск новых серверов для оставшихся задач..." << std::endl;
            discoverServers();
        }
    }

    return total_result;
}

int main(int argc, char* argv[]) {
    try {
        if (argc != 4) {
            std::cerr << "Использование: " << argv[0] << " <start> <end> <step>" << std::endl;
            return 1;
        }

        double start = std::stod(argv[1]);
        double end = std::stod(argv[2]); 
        double step_for_integral = std::stod(argv[3]);

        std::vector<Task> tasks;

        double step_for_worker = (end - start) / 4;
        
        for (double x = start; x < end; x += step_for_worker) {
            Task task;
            task.start = x;
            task.end = std::min(x + step_for_worker, end);
            task.step = step_for_integral;
            tasks.push_back(task);
        }

        std::cout << "Начало работы мастера" << std::endl;
        // sleep(10); // Для test_udp_network_loss.sh
        discoverServers();
        
        double result = distribute_tasks(tasks);

        std::cout << "\nИтоговый результат: " << result << std::endl;
        
    } catch (const std::exception& e) {
        std::cerr << "Ошибка: " << e.what() << std::endl;
        return 1;
    }
    
    return 0;
}
