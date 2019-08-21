inisial upoad

djangows [inprogress]
minta schema table ~ rz [inprogress]
create model from table ~ rz [inprogress]
create serializer from model ~ rz [inprogress]
create view based on output and prosess ~ rz [pending]

-- schedule optimization --
1. READ PARAMETER  ~ kho [inprogress]
class: DataLoader, Truck 
2. SORTING SPBU LIST BY CRITICAL TIME & ZONATION [inprogress]
class: Order ~ kho
class : DemandForecaster ~ hanafi
3. SET PRIORITY & OUTSTANDING [pending]
class : vehicle router
function: priority & outstanding vehicle assignment
4. CALCULATE CAPACITY & REQUEST [pending]
class : order
function : aggregate order by shift
5. ROUTE OPTIMIZATION [pending]
class : vehicle router
class : ritase schedule
6. ESTIMATE RETURN TIME [pending]
class : traveltime calculator


