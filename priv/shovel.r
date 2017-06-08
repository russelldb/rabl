shovel_latencies <- function(dir1, dir2) {
 d1tod2 <- shovel_latency(dir1, dir2)
 d2tod1 <- shovel_latency(dir2, dir1)
 df <- data.frame(quantile(d1tod2$latency, c(0, .1, .5, .95, .99, 1)),
                  quantile(d2tod1$latency, c(0, .1, .5, .95, .99, 1)))
 names(df) <- c(paste(dir1, "to", dir2, sep=" "),
                paste(dir2, "to", dir1, sep=" "))
 df
}

files_to_df <- function(dir, filter, col2) {
 files <- list.files(path=dir, pattern=filter,full.names=TRUE)
 frames <- do.call(rbind, lapply(files, read.csv, head=FALSE,sep=" "))
 names(frames) <- c("TAG", col2)
 frames
}

shovel_latency <- function(source, sink) {
 publishes <- files_to_df(source, "*publish", "PUB")
 consumes <- files_to_df(sink, "*consume", "CON")
 p2c <- merge(publishes, consumes, by="TAG")
 p2c$latency <- p2c$CON - p2c$PUB
 p2c[order(p2c$CON),]
}