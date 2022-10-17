-- Include a .sql file with queries to answer these questions:
-- From the two most commonly appearing regions, which is the latest datasource?
-- What regions has the "cheap_mobile" datasource appeared in?

select datetime,datasource,region from trips where region in (select region from trips group by region order by count() desc limit 2) order by datetime desc limit 1;

select distinct region,datasource from trips where datasource like "cheap_mobile";