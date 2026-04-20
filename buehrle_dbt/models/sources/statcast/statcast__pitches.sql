{{
    config(
        materialized='table',
        enabled=false
    )
}}

with statcast as (

    select
        cast(game_pk as int64) as game_id
      , cast(game_date as date) as game_date
      , cast(game_year as int64) as game_year
        -- E = EX, S = ST, R = RS, F = WC, D = DS, L = LC, W = WS
      , cast(game_type as string) as game_type
      , cast(home_team as string) as home_team
      , cast(away_team as string) as away_team
      , cast(home_score as int64) as home_score
      , cast(away_score as int64) as away_score
      , cast(post_home_score as int64) as post_home_score
      , cast(post_away_score as int64) as post_away_score
      , cast(bat_score as int64) as batting_team_score
      , cast(fld_score as int64) as fielding_team_score
      , cast(stand as string) as batter_stand
      , cast(p_throws as string) as pitcher_hand
      , cast(inning as int64) as inning
      , cast(inning_topbot as string) as half_inning
      , cast(at_bat_number as int64) as game_plate_appearance
      , cast(pitch_number as int64) as pitch_number
      , cast(balls as int64) as balls
      , cast(strikes as int64) as strikes
      , cast(outs_when_up as int64) as outs
      , cast(if_fielding_alignment as string) as infield_alignment
      , cast(of_fielding_alignment as string) as outfield_alignment
      , cast(batter as int64) as batter_mlbam_id
      , cast(pitcher as int64) as pitcher_mlbam_id
      , cast(fielder_2 as int64) as catcher_mlbam_id
      , cast(fielder_3 as int64) as first_base_mlbam_id
      , cast(fielder_4 as int64) as second_base_mlbam_id
      , cast(fielder_5 as int64) as third_base_mlbam_id
      , cast(fielder_6 as int64) as shortstop_mlbam_id
      , cast(fielder_7 as int64) as left_field_mlbam_id
      , cast(fielder_8 as int64) as center_field_mlbam_id
      , cast(fielder_9 as int64) as right_field_mlbam_id
      , cast(on_1b as int64) as runner_on_first_mlbam_id
      , cast(on_2b as int64) as runner_on_second_mlbam_id
      , cast(on_3b as int64) as runner_on_third_mlbam_id
      , cast(pitch_type as string) as pitch_type
      , cast(pitch_name as string) as pitch_name
      , cast(sz_top as numeric) as strike_zone_top
      , cast(sz_bot as numeric) as strike_zone_bottom
      , cast(release_speed as numeric) as release_speed
      , cast(release_spin_rate as int64) as release_spin_rate
      , cast(spin_axis as int64) as spin_axis
      , cast(release_extension as numeric) as release_extension
      , cast(release_pos_x as numeric) as release_position_x
      , cast(release_pos_y as numeric) as release_position_y
      , cast(release_pos_z as numeric) as release_position_z
      , cast(vx0 as numeric) as velocity_50_x
      , cast(vy0 as numeric) as velocity_50_y
      , cast(vz0 as numeric) as velocity_50_z
      , cast(ax as numeric) as acceleration_50_x
      , cast(ay as numeric) as acceleration_50_y
      , cast(az as numeric) as acceleration_50_z
      , cast(pfx_x as numeric) as movement_x
      , cast(pfx_z as numeric) as movement_z
      , cast(plate_x as numeric) as plate_x
      , cast(plate_z as numeric) as plate_z
      , cast(zone as numeric) as strike_zone_location
      , cast(effective_speed as numeric) as effective_speed
      , cast(events as string) as plate_appearance_result
      , cast(des as string) as plate_appearance_result_description
      , cast(type as string) as pitch_result
      , cast(description as string) as pitch_result_description
      , cast(bb_type as string) as batted_ball_type
      , cast(hit_location as int64) as initial_fielder
      , cast(hc_x as numeric) as hit_coordinate_x
      , cast(hc_y as numeric) as hit_coordinate_y
      , cast(hit_distance_sc as numeric) as hit_distance
      , cast(launch_speed as numeric) as exit_velocity
      , cast(launch_angle as int64) as launch_angle
      , cast(launch_speed_angle as int64) as launch_speed_angle
      , cast(estimated_ba_using_speedangle as numeric) as estimated_batting_average
      , cast(estimated_woba_using_speedangle as numeric) as estimated_woba
      , cast(woba_value as numeric) as woba_value
      , cast(woba_denom as numeric) as woba_denominator
      , cast(babip_value as int64) as babip_value
      , cast(iso_value as int64) as number_of_extra_bases
      , cast(delta_home_win_exp as numeric) as home_win_expectancy_delta
      , cast(delta_run_exp as numeric) as run_expectancy_delta
    
    from {{ source('statcast', 'pitches') }}

),

chadwick as (

    select * from {{ ref('chadwick__register') }}

),

transformed as (

    select
        {{ dbt_utils.generate_surrogate_key([
            'statcast.game_id',
            'statcast.game_plate_appearance',
            'statcast.pitch_number'
        ]) }} as pitch_id
      , statcast.game_id
      , statcast.game_date
      , statcast.game_year
      , statcast.game_type
      , statcast.home_team
      , statcast.away_team
      , statcast.home_score
      , statcast.away_score
      , statcast.post_home_score
      , statcast.post_away_score
      , statcast.batting_team_score
      , statcast.fielding_team_score
      , statcast.batter_stand
      , statcast.pitcher_hand
      , statcast.inning
      , statcast.half_inning
      , statcast.game_plate_appearance
      , statcast.pitch_number
      , statcast.balls
      , statcast.strikes
      , statcast.outs
      , statcast.infield_alignment
      , statcast.outfield_alignment
      , batter.person_id as batter_person_id
      , pitcher.person_id as pitcher_person_id
      , catcher.person_id as catcher_person_id
      , first_base.person_id as first_base_person_id
      , second_base.person_id as second_base_person_id
      , third_base.person_id as third_base_person_id
      , shortstop.person_id as shortstop_person_id
      , left_field.person_id as left_field_person_id
      , center_field.person_id as center_field_person_id
      , right_field.person_id as right_field_person_id
      , runner_on_first.person_id as runner_on_first_person_id
      , runner_on_second.person_id as runner_on_second_person_id
      , runner_on_third.person_id as runner_on_third_person_id
      , statcast.pitch_type
      , statcast.pitch_name
      , statcast.strike_zone_top
      , statcast.strike_zone_bottom
      , statcast.release_speed
      , statcast.release_spin_rate
      , statcast.spin_axis
      , statcast.release_extension
      , statcast.release_position_x
      , statcast.release_position_y
      , statcast.release_position_z
      , statcast.velocity_50_x
      , statcast.velocity_50_y
      , statcast.velocity_50_z
      , statcast.acceleration_50_x
      , statcast.acceleration_50_y
      , statcast.acceleration_50_z
      , statcast.movement_x
      , statcast.movement_z
      , statcast.plate_x
      , statcast.plate_z
      , statcast.strike_zone_location
      , statcast.effective_speed
      , statcast.plate_appearance_result
      , statcast.plate_appearance_result_description
      , statcast.pitch_result
      , statcast.pitch_result_description
      , statcast.batted_ball_type
      , statcast.initial_fielder
      , statcast.hit_coordinate_x
      , statcast.hit_coordinate_y
      , statcast.hit_distance
      , statcast.exit_velocity
      , statcast.launch_angle
      , statcast.launch_speed_angle
      , statcast.estimated_batting_average
      , statcast.estimated_woba
      , statcast.woba_value
      , statcast.woba_denominator
      , statcast.babip_value
      , statcast.number_of_extra_bases
      , statcast.home_win_expectancy_delta
      , statcast.run_expectancy_delta

    from statcast
    left join chadwick as batter
        on statcast.batter_mlbam_id = batter.mlbam_id
    left join chadwick as pitcher
        on statcast.pitcher_mlbam_id = pitcher.mlbam_id
    left join chadwick as catcher
        on statcast.catcher_mlbam_id = catcher.mlbam_id
    left join chadwick as first_base
        on statcast.first_base_mlbam_id = first_base.mlbam_id
    left join chadwick as second_base
        on statcast.second_base_mlbam_id = second_base.mlbam_id
    left join chadwick as third_base
        on statcast.third_base_mlbam_id = third_base.mlbam_id
    left join chadwick as shortstop
        on statcast.shortstop_mlbam_id = shortstop.mlbam_id
    left join chadwick as left_field
        on statcast.left_field_mlbam_id = left_field.mlbam_id
    left join chadwick as center_field
        on statcast.center_field_mlbam_id = center_field.mlbam_id
    left join chadwick as right_field
        on statcast.right_field_mlbam_id = right_field.mlbam_id
    left join chadwick as runner_on_first
        on statcast.runner_on_first_mlbam_id = runner_on_first.mlbam_id
    left join chadwick as runner_on_second
        on statcast.runner_on_second_mlbam_id = runner_on_second.mlbam_id
    left join chadwick as runner_on_third
        on statcast.runner_on_third_mlbam_id = runner_on_third.mlbam_id

)

select * from transformed