shovel_latencies <- function(dir1, dir2) {

 s2to1 <- shovel_latency(dir1, dir2)
 s1to2 <- shovel_latency(dir2, dir1)
 df <- data.frame(quantile(s2to1$latency, c(0, .1, .5, .95, .99, 1)),
            quantile(s1to2$latency, c(0, .1, .5, .95, .99, 1)))
 names(df) <- c(paste(dir1, "to", dir2, sep=" "), paste(dir2, "to", dir1, sep=" "))
 df
}

files_to_df <- function(dir, filter, col2) {
 files <- list.files(path=dir, pattern=filter,full.names=TRUE)
 frames <- do.call(rbind, lapply(files, read.csv, head=FALSE,sep=" "))
 names(frames) <- c("TAG", col2)
 frames
}

shovel_latency <- function(dir1, dir2) {
 s1c <- files_to_df(dir2, "*consume", "CON")
 s2p <- files_to_df(dir1, "*publish", "PUB")
 s2to1 <- merge(s2p, s1c, by="TAG")
 s2to1$latency <- s2to1$CON - s2to1$PUB
 s2to1
}