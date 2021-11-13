SELECT * FROM project1.housing_data;


---------------------------------------------------------------------------------------------------------
-- STANDARDIZE DATE FORMAT BY CONVERTING THEM TO DATE TYPE 

select SaleDate, cast(saledate as date) 
from housing_data; 

set sql_safe_updates = 0; 

update housing_data
set saledate = cast(saledate as date); 

-- If it doesn't update properly, add SaleDateConverted with date type 
alter table housing_data
add SaleDateConverted date; 

update housing_data 
set SaleDateConverted = cast(SaleDate as Date); 


---------------------------------------------------------------------------------------------------------
-- POPULATE PROPERTY ADDRESS DATA 

update housing_data
set propertyaddress = null 
where propertyaddress = '';

select * 
from housing_data
where PropertyAddress is null;

select a.parcelid, a.propertyaddress, b.parcelid, b.propertyaddress, ifnull(a.propertyaddress, b.propertyaddress)
from housing_data a 
join housing_data b 
	on a.parcelid = b.ParcelID
    and a.UniqueID <> b.UniqueID
where a.PropertyAddress is null; 

update housing_data a
inner join housing_data b
	on (a.ParcelID = b.ParcelID and a.UniqueID <> b.UniqueID)
set a.propertyaddress = ifnull(a.propertyaddress, b.propertyaddress) 
where a.propertyaddress is null; 


---------------------------------------------------------------------------------------------------------
-- CHANGE Y AND N TO YES AND NO IN "SOLD AS VACANT" FIELD 

select distinct(soldasvacant), count(soldasvacant) 
from housing_data
group by SoldAsVacant; 

select soldasvacant, 
case 
	when soldasvacant = 'y' then 'Yes'
    when soldasvacant = 'n' then 'No'
    else soldasvacant
    end 
from housing_data; 
    
update housing_data
set soldasvacant = case when soldasvacant = 'y' then 'Yes'
						when soldasvacant = 'n' then 'No'
						else soldasvacant
						end; 
                        
                        
---------------------------------------------------------------------------------------------------------
-- SPLIT ADDRESS INTO INDIVIDUAL COLUMNS (ADDRESS, CITY, STATE) 

----------------------------------------------------------------
-- Split Property Address into Property Address and Property City

select PropertyAddress, substring_index(propertyaddress, ',', 1) as Address, 
	substring_index(propertyaddress, ',', -1) as City
from housing_data; 

alter table housing_data
add PropertySplitAddress varchar(250); 

update housing_data
set PropertySplitAddress = substring_index(propertyaddress, ',', 1); 

alter table housing_data
add PropertySplitCity varchar(250); 

update housing_data
set PropertySplitCity = substring_index(propertyaddress, ',', -1); 

select * 
from housing_data; 

--------------------------------------------------------------------
-- Split Owner Address into Owner Address, Owner City and Owner State 

select owneraddress, substring_index(owneraddress, ',', 1) as OwnerSplitAddress, 
	substring_index(substring_index(owneraddress, ',', 2), ',', -1) as OwnerSplitCity, 
	substring_index(owneraddress, ',', -1) as OwnerSplitState
from housing_data; 

alter table housing_data
add OwnerSplitAddress varchar(250), 
add OwnerSplitCity varchar(250),
add OwnerSplitState varchar(250); 

update housing_data
set OwnerSplitAddress = substring_index(owneraddress, ',', 1); 

update housing_data
set OwnerSplitCity = substring_index(substring_index(owneraddress, ',', 2), ',', -1); 

update housing_data
set OwnerSplitState = substring_index(owneraddress, ',', -1); 

select * 
from housing_data; 


---------------------------------------------------------------------------------------------------------
-- REMOVE DUPLICATES 

select UniqueID
from (
	select UniqueID, 
		row_number() over(
				partition by ParcelID, PropertyAddress, SaleDate, SalePrice, LegalReference 
				order by UniqueID) row_num
	from housing_data) housing_data_1
where row_num > 1; 

delete from housing_data
where UniqueID in (
	select UniqueID
	from (
		select UniqueID, 
			row_number() over(
				partition by ParcelID, PropertyAddress, SaleDate, SalePrice, LegalReference 
				order by UniqueID) row_num
		from housing_data) housing_data_1
	where row_num > 1) ; 

select * 
from housing_data; 












