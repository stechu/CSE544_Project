scalability <- function () {
  input <- read.csv('../deploy/scalability.csv')
  ggplot(input,aes(x=machines,y=time/60)) +
      geom_line(aes(color='Results')) +
      geom_point(aes(color='Results')) +
      stat_smooth(aes(color='Fitted O(1/n)'),method='nls',formula=y~1/x*a+b,start=list(a=1,b=1),se=F) +
      scale_colour_discrete('') +
      scale_y_continuous(limits=c(0,55)) +
      labs(x='# Machines',y='Time to Completion [min]') +
      opts(title='Scalability of our Implementation on Multiple Machines\nN=2200')
}

corescale <- function () {
  input <- read.csv('../deploy/corescale.csv')
  ggplot(input,aes(x=cores,y=time)) +
    geom_line(aes(color='Results')) +
    geom_point(aes(color='Results')) +
    stat_smooth(aes(color='Fitted O(1/n)'),method='nls',formula=y~1/x*a+b,start=list(a=1,b=1),se=F) +
    scale_colour_discrete('') +
    scale_y_continuous(limits=c(0,500)) +
    scale_x_continuous(breaks=c(8,16,24,32)) +
    labs(x='# Cores',y='Time to Completion [s]') +
    opts(title='Scalability of our Implementation on Single Machine\nN=500')
}

onehour <- function () {
  input <- read.csv('../deploy/onehour.csv')
  print(input)
  #stat_smooth(method='lm',se=F) +
  ggplot(input,aes(x=N,y=time/60)) +
    geom_point(aes(color='Results')) +
    stat_smooth(aes(color='Fitted O(n²)'),method='nls',formula=y~x*x*a+b,start=list(a=1,b=1),se=F) +
    scale_colour_discrete('') +
    scale_y_continuous(limits=c(0,65)) +
    scale_x_continuous(limits=c(1000,7000)) +
    labs(x='# Seed Papers',y='Time to Completion [min]') +
    opts(title='Completion Time for Varying Numbers of Seed Papers\n16 Machines')
}
