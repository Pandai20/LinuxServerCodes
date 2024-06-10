
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#ifndef NULL
#define NULL 0
#endif

#define PUBLIC 

PUBLIC struct ctrip_task
{
    struct ctrip_task* pNext;
    int                  value;
};

struct ctrip_thread_info
{
    //线程退出标志
    int                    thread_running;
    int                    thread_num;
    int                    tasknum;
    struct ctrip_task* tasks;
    pthread_t* threadid;
    pthread_mutex_t        mutex;
    pthread_cond_t         cond;
};

/* 初始化线程池线程数目
 * @param thread_num 线程数目, 默认为8个
 */
PUBLIC void  ctrip_init_thread_pool(int thread_num);

/* 销毁线程池
 */
PUBLIC void  ctrip_destroy_thread_pool();

/**向任务池中增加一个任务
 * @param t 需要增加的任务
 */
PUBLIC void ctrip_thread_pool_add_task(struct ctrip_task* t);

/**从任务池中取出一个任务
 * @return 返回得到的任务
 */
struct ctrip_task* ctrip_thread_pool_retrieve_task();

/**执行任务池中的任务
 * @param t 需要执行的任务
 */
PUBLIC void ctrip_thread_pool_do_task(struct ctrip_task* t);

/**线程函数
 * @param thread_param 线程参数
 */
void* ctrip_thread_routine(void* thread_param);



struct ctrip_thread_info g_threadinfo;
int thread_running = 0;

void ctrip_init_thread_pool(int thread_num)
{
    if (thread_num <= 0)
        thread_num = 5;

    pthread_mutex_init(&g_threadinfo.mutex, NULL);
    pthread_cond_init(&g_threadinfo.cond, NULL);

    g_threadinfo.thread_num = thread_num;
    g_threadinfo.thread_running = 1;
    g_threadinfo.tasknum = 0;
    g_threadinfo.tasks = NULL;
    thread_running = 1;

    g_threadinfo.threadid = (pthread_t*)malloc(sizeof(pthread_t) * thread_num);

    int i;
    for (i = 0; i < thread_num; ++i)
    {
        pthread_create(&g_threadinfo.threadid[i], NULL, ctrip_thread_routine, NULL);
    }
}

void ctrip_destroy_thread_pool()
{
    g_threadinfo.thread_running = 0;
    thread_running = 0;
    pthread_cond_broadcast(&g_threadinfo.cond);

    int i;
    for (i = 0; i < g_threadinfo.thread_num; ++i)
    {
        pthread_join(g_threadinfo.threadid[i], NULL);
    }

    free(g_threadinfo.threadid);

    pthread_mutex_destroy(&g_threadinfo.mutex);
    pthread_cond_destroy(&g_threadinfo.cond);
}

void ctrip_thread_pool_add_task(struct ctrip_task* t)
{
    if (t == NULL)
        return;

    pthread_mutex_lock(&g_threadinfo.mutex);

    struct ctrip_task* head = g_threadinfo.tasks;
    if (head == NULL)
        g_threadinfo.tasks = t;
    else
    {
        while (head->pNext != NULL)
        {
            head = head->pNext;
        }

        head->pNext = t;
    }

    ++g_threadinfo.tasknum;
    //当有变化后，使用signal通知wait函数
    pthread_cond_signal(&g_threadinfo.cond);
    pthread_mutex_unlock(&g_threadinfo.mutex);
}


struct ctrip_task* ctrip_thread_pool_retrieve_task()
{
    struct ctrip_task* head = g_threadinfo.tasks;
    if (head != NULL)
    {
        g_threadinfo.tasks = head->pNext;
        --g_threadinfo.tasknum;
        printf("retrieve a task, task value is [%d]\n", head->value);
        return head;
    }

    printf("no task\n");

    return NULL;
}

void* ctrip_thread_routine(void* thread_param)
{
    printf("thread NO.%d start.\n", (int)pthread_self());

    while (thread_running/*g_threadinfo.thread_running*/)
    {
        struct ctrip_task* current = NULL;

        pthread_mutex_lock(&g_threadinfo.mutex);

        while (g_threadinfo.tasknum <= 0)
        {
            //如果获得了互斥锁，但是条件不合适的话，wait会释放锁，不往下执行。
            //当变化后，条件合适，将直接获得锁。
            pthread_cond_wait(&g_threadinfo.cond, &g_threadinfo.mutex);

            if (!g_threadinfo.thread_running)
                break;

            //current = ctrip_thread_pool_retrieve_task();

            if (current != NULL)
                break;
        }// end inner-while-loop
        current = ctrip_thread_pool_retrieve_task();
        pthread_mutex_unlock(&g_threadinfo.mutex);

        ctrip_thread_pool_do_task(current);
    }// end outer-while-loop

    printf("thread NO.%d exit.\n", (int)pthread_self());
}


void ctrip_thread_pool_do_task(struct ctrip_task* t)
{
    if (t == NULL)
        return;


    //TODO: do your work here
    printf("task value is [%d]\n", t->value);
    //TODO：如果t需要释放，记得在这里释放
}
