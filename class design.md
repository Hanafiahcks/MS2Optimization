Object List : 
--Optimisation Engine--
step - 1. Initialize Data
1. DataLoader
    __init__:
        load all table to pandas dataframe
    attribute : 
        1 table = 1 dataframe      
2. Truck
    dict_truck = {
        "id (nopol)",
        "Capacity",
        "kompartemen" = [4,4,8]
        "status_kompartemen" = [False,False,True]  >> status terisi atau tidak     
        "status_PTO" = True/False
    }
    rit_history  = [
        {
            
        }
    ]

3. Order
    dict_order={
        "order_id",
        "spbu",
        "spbu_distance",   >> dari t_distance_travel_time       
        "product",
        "volume",
        "rec_shift",     >> shift 1, shift 2, shift 3
        "critical_time",  >> dari hasil forecast
        "cum_vol" #per shift per spbu per product        
    }

    def sort_order () > by spbu_distance & by critical_time, per shift
    def aggregate_order_per_spbu

step 2 - 2. Order Sorting by location & critical time
1. DemandForecaster
    forecast:
    output > sama seperti existing  

Step 3 : Optimisation
1. VehicleRouter
    initial_truck_queue
    returned_truck_queue
    timeticker
    rit_record = {}  --> same like truck_temp
    def truck_assignment
    
2. FuelFillingProcessor
    filling_bay_num = 4 
    processing_record = {
        "process_time",
        "bay_num",
        "truck_id",
        "rit_id",
    }
    def filling_process

3. TravelTimeCalculator

    
4. ResultEvaluator      > for the sake of testing by pak Hisyam
    def count_rit
    def calculate_total_distance
    def calculate_total_deadfreight

5. ResultExporter
    def export_excel    > for the sake of testing by pak Hisyam
    def export_db       > export to existing table schema

--Integration API--
Flask REST API

Task List : 
1. Setup Environment
    - versioning di git
    - python 3.7
    - pandas 
    -     
2. Restore DB
3. Development
    3.1. Load Data
    3.2. Logic
    3.3. Write to DB
    3.4. REST API