# TASK 1
library(data.table)
library(nycflights13)

# Load the flights dataset
data("flights")

# Convert to data.table
flights_dt <- as.data.table(flights)

# Compute average departure delay for each airline
avg_dep_delay <- flights_dt[, .(avg_dep_delay = mean(dep_delay, na.rm = TRUE)), by = carrier]
print(avg_dep_delay)

# Find top 5 destinations with the most flights
top_destinations <- flights_dt[, .N, by = dest][order(-N)][1:5]
print(top_destinations)


# Create a unique ID for each flight
flights_dt[, unique_id := .I]

# Create a column indicating if the flight was delayed for more than 15 minutes
flights_dt[, delayed := ifelse(dep_delay > 15, TRUE, FALSE)]


# Save the processed data as a CSV file
fwrite(flights_dt, "processed_flights.csv")

# Alternatively, save to an SQLite database
library(RSQLite)
con <- dbConnect(SQLite(), "flights.db")
dbWriteTable(con, "flights", flights_dt)
dbDisconnect(con)

#Task 2

library(ambiorix)
library(jsonlite)
library(DBI)
library(RSQLite)

db <- dbConnect(SQLite(), "flights.sqlite")
dbWriteTable(db, "flights", flights_dt, overwrite = TRUE)
app <- Ambiorix$new()


app$add_post("/flight", function(req) {
  data <- fromJSON(req$body)
  dbWriteTable(db, "flights", as.data.table(data), append = TRUE)
  list(status = "Flight added")
})




app$add_get("/flight/:id", function(req) {
  id <- as.integer(req$path_params$id)
  res <- dbGetQuery(db, paste0("SELECT * FROM flights WHERE flight_id = ", id))
  toJSON(res, auto_unbox = TRUE)
})



app$add_get("/check-delay/:id", function(req) {
  id <- as.integer(req$path_params$id)
  res <- dbGetQuery(db, paste0("SELECT delayed FROM flights WHERE flight_id = ", id))
  toJSON(list(delayed = res$delayed[1]), auto_unbox = TRUE)
})


app$add_get("/avg-dep-delay", function(req) {
  airline <- req$query_params$id
  if (!is.null(airline)) {
    res <- dbGetQuery(db, paste0("SELECT avg(dep_delay) AS avg_delay FROM flights WHERE carrier = '", airline, "'"))
  } else {
    res <- dbGetQuery(db, "SELECT carrier, avg(dep_delay) AS avg_delay FROM flights GROUP BY carrier")
  }
  toJSON(res, auto_unbox = TRUE)
})



app$add_get("/top-destinations/:n", function(req) {
  n <- as.integer(req$path_params$n)
  res <- dbGetQuery(db, paste0("SELECT dest, COUNT(*) AS count FROM flights GROUP BY dest ORDER BY count DESC LIMIT ", n))
  toJSON(res, auto_unbox = TRUE)
})




app$add_put("/flights/:id", function(req) {
  id <- as.integer(req$path_params$id)
  data <- fromJSON(req$body)
  dbExecute(db, paste0("UPDATE flights SET dep_delay = ", data$dep_delay, " WHERE flight_id = ", id))
  list(status = "Updated successfully")
})




app$start(port = 8080)





