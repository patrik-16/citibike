UPDATE citibike_trips
SET start_lat = start_lat / 1000000.0, 
    start_lng = start_lng / 1000000.0,
    end_lat   = end_lat   / 1000000.0,
    end_lng   = end_lng   / 1000000.0;
