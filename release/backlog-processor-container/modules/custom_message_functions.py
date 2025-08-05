# This function includes the custom logic for creating calculated signals
# You can update/extend the logic as needed - or use one of the existing examples
def apply_custom_function(df_messages, function):   
    from .utils import check_geofence
    
    # Example 1: Combine multiple DM01 messages into a single message incl. the SA and 'final' SPN    
    if function == "combine_dtcs":
        df_messages["SA"] = df_messages["Message"].apply(lambda x: int(x[-2:], 16))
        df_messages["DM01_SPN_Final"] =  df_messages["DM01_SPN"] + df_messages["DM01_SPN_High"]
        df_messages = df_messages.sort_index() 
    
    # Example 2: Add custom geofences 
    if function == "custom_geofences":
        geofences = [(1,"Area1",(56.07270599999998,10.103397999999999),0.2),(2,"Area2",(56.116626,10.154563999999993),0.3)]
        df_messages["GeofenceId"] = df_messages.apply(check_geofence, axis=1, args=("Latitude", "Longitude", geofences))
        signals_to_include = ["GeofenceId"]
        df_messages = df_messages[signals_to_include]
        df_messages = df_messages[(df_messages.notnull()).any(axis=1)]
           
     # Example 3: Add trip distance delta signals
    if function == "delta_distance":
        df_messages['DeltaDistance'] = df_messages['DistanceTrip'].diff()
        df_messages["DeltaDistanceHighSpeed"] = df_messages['DeltaDistance'].where((df_messages['Speed'] > 20) & (df_messages['SpeedValid'] == 1), None)
        df_messages["DeltaDistanceLowSpeed"] = df_messages['DeltaDistance'].where((df_messages['Speed'] <= 20) & (df_messages['SpeedValid'] == 1), None)           
        signals_to_include = ["DeltaDistance","DeltaDistanceHighSpeed","DeltaDistanceLowSpeed"]
        df_messages = df_messages[signals_to_include]
        df_messages = df_messages[(df_messages.notnull()).any(axis=1)]
    
    # Example 4: Simply return the original data frame to resample it with no other modifications
    if function == "resample":
        pass
    
    
    # Drop the 'Message' column that was added to enable specialized customization use cases
    try:
        df_messages = df_messages.drop(columns=["Message"]) 
    except:
        pass
                         
    return df_messages