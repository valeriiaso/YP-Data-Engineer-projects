

create table analysis.dm_rfm_segments ( 
user_id int not null, 
recency int not null check(recency >= 1 and recency <= 5), 
frequency int not null check(frequency >= 1 and frequency <= 5), 
monetary_value int not null check(monetary_value >= 1 and monetary_value <= 5), 
constraint dm_rfm_segments_pkey primary key (user_id)
); 