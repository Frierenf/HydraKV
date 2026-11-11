#ifndef UTIL_TIMER_H_
#define UTIL_TIMER_H_

#include <stdint.h>
#include <time.h>
#include<chrono>

#define RDTSC_CLOCK
#define CPU_SPEED_MHZ (2900)

class Timer {
public:
    unsigned long long asm_rdtsc(void)
    {
        unsigned hi, lo;
        __asm__ __volatile__("rdtsc"
                             : "=a"(lo), "=d"(hi));
        return ((unsigned long long)lo) | (((unsigned long long)hi) << 32);
    }

    unsigned long long asm_rdtscp(void)
    {
        unsigned hi, lo;
        __asm__ __volatile__("rdtscp"
                             : "=a"(lo), "=d"(hi)::"rcx");
        return ((unsigned long long)lo) | (((unsigned long long)hi) << 32);
    }

    uint64_t cycles_to_ns(int cpu_speed_mhz, uint64_t cycles)
    {
        return (cycles * 1000 / cpu_speed_mhz);
    }

    uint64_t ns_to_cycles(int cpu_speed_mhz, uint64_t ns)
    {
        return (ns * cpu_speed_mhz / 1000);
    }

public:
    Timer(void)
        : elapsed{ 0 }
    {
    }

    void Start(void)
    {
#if (defined RDTSC_CLOCK)
        start = asm_rdtscp();
#else
        clock_gettime(CLOCK_MONOTONIC, &start);
#endif
    }

    void Stop(void)
    {
#if (defined RDTSC_CLOCK)
        end = asm_rdtscp();
        elapsed = end - start;
#else
        clock_gettime(CLOCK_MONOTONIC, &end);
        elapsed = (end.tv_sec - start.tv_sec) * 1000000000 + (end.tv_nsec - start.tv_nsec);
#endif
    }

    size_t Get(void)
    {
#if (defined RDTSC_CLOCK)
        return cycles_to_ns(CPU_SPEED_MHZ, elapsed);
#else
        return elapsed;
#endif
    }

    double GetSeconds(void)
    {
#if (defined RDTSC_CLOCK)
        return cycles_to_ns(CPU_SPEED_MHZ, elapsed) / 1000000000.0;
#else
        return elapsed / 1000000000.0;
#endif
    }

    size_t Now(void)
    {
#if (defined RDTSC_CLOCK)
        return 0;
#else
        clock_gettime(CLOCK_MONOTONIC, &now);
        return now.tv_sec * 1000000000 + now.tv_nsec;
#endif
    }

    void Accumulate(void)
    {
#if (defined RDTSC_CLOCK)
#else
        clock_gettime(CLOCK_MONOTONIC, &end);
        elapsed += (end.tv_sec - start.tv_sec) * 1000000000 + (end.tv_nsec - start.tv_nsec);
#endif
    }

private:
#if (defined RDTSC_CLOCK)
    uint64_t start, end;
#else
    struct timespec start, end, now;
#endif
    size_t elapsed;
};
namespace utils {

    template <typename T>
    class Timer {
    public:
        void Start() {
            time_ = Clock::now();
        }

        void Reset() {
            time_ = Clock::now();
        }

        T End() {
            Duration span;
            Clock::time_point t = Clock::now();
            span = std::chrono::duration_cast<Duration>(t - time_);
            return span.count();
        }

    private:
        typedef std::chrono::high_resolution_clock Clock;
        typedef std::chrono::duration<T> Duration;

        Clock::time_point time_;
    };

} // utils
#endif // UTIL_TIMER_H_