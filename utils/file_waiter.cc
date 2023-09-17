#include <cstdio>
#include <cstdlib>
#include <errno.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/inotify.h>

int main(int argc, char **argv) {
  const unsigned int EVENT_SIZE = sizeof(struct inotify_event);
  const unsigned int BUF_LEN = 1024 * (EVENT_SIZE + 16);
  char buffer[BUF_LEN];

  if (argc < 1)
    return 1;

  int fd = inotify_init();

  if (fd < 0) {
    perror("inotify_init");
    return 1;
  }

  int wd = inotify_add_watch(fd, argv[1], IN_CREATE | IN_CLOSE_WRITE | IN_MOVED_TO);
  printf("Watching %s for new files\n", argv[1]);
  for (;;) {
    int length = read(fd, buffer, BUF_LEN);

    if (length < 0) {
      perror("read");
      return 2;
    }

    for (int i = 0; i < length;) {
      struct inotify_event *event = reinterpret_cast<inotify_event *>(&buffer[i]);
      if (event->len) {
        if (event->mask & IN_CREATE) {
          if (event->mask & IN_ISDIR) {
            printf("The directory %s was created.\n", event->name);
          } else {
            printf("The file %s was created.\n", event->name);
          }
        } else if (event->mask & IN_CLOSE_WRITE) {
          if (event->mask & IN_ISDIR) {
            printf("The directory %s was closed after writing.\n", event->name);
          } else {
            printf("The file %s was closed after writing.\n", event->name);
          }
        } else if (event->mask & IN_MOVED_TO) {
          if (event->mask & IN_ISDIR) {
            printf("The directory %s was moved to here.\n", event->name);
          } else {
            printf("The file %s was moved to here.\n", event->name);
          }
        }
      }
      i += EVENT_SIZE + event->len;
    }
  }

  inotify_rm_watch(fd, wd);
  close(fd);

  return 0;
}