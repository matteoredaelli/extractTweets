#!/usr/bin/env Rscript

library(knitr)

args <- commandArgs(TRUE)
if (length(args) < 3) stop("Bad args, usage title input output")

title <- args[1]
source_path <- args[2]
target_path <- args[3]
skip.tweets <- ifelse(is.na(args[4]), FALSE, TRUE)

print(skip.tweets)

template_file = "tweets-report-template.Rhtml"

page = readChar(template_file, file.info(template_file)$size)
page = gsub("__TITLE__", title, page)

knit(text=page, output=target_path)
