#  
# => has_activity
# => Cary Dunn <cary.dunn@gmail.com>
#  

# HasActivity

require 'core_ext'

module Elctech
  module Has #:nodoc:
    module Activity #:nodoc:
      def self.included(base)
        base.extend(ClassMethods)
      end
      
      module ClassMethods
        
        def self.extended(base)
          base.class_inheritable_accessor :activity_options
        end
        
        def has_activity(options={})
          options[:by] ||= "created_at"
          include Elctech::Has::Activity::InstanceMethods
          extend Elctech::Has::Activity::SingletonMethods
          
          self.activity_options = options
        end
      end
      
      module SingletonMethods
        
        # Grabs a hash of the activity since <time ago> grouped by <hour/day/week>
        # 
        #   * :conditions
        #       same as the standard Rails finder. Used to scope your activity to a particular user, etc.
        #   * :padding
        #       true/false
        #   * :group_by
        #       :hour, :day, :week
        # 
        def activity_since(since=1.week.ago, options={})
          activity_scope = (options.has_key?(:conditions) ? sanitize_sql(options[:conditions]) : "1=1")
          options[:padding] ||= true
          options[:order] ||= :asc
          options[:group_by] ||= :day
          
          db_adapter_name = ActiveRecord::Base::connection.instance_variable_get(:@config)[:adapter]
          
          case options[:group_by]
          when :hour
            if db_adapter_name == 'mysql'
              sql_statement = sanitize_sql(
                ["SELECT
                    COUNT(*) AS activity_count,
                    ((((YEAR(now()) - YEAR(#{activity_options[:by]}))*365)+(DAYOFYEAR(now())-DAYOFYEAR(#{activity_options[:by]})))*24)+(HOUR(now())-HOUR(#{activity_options[:by]})) as hours_ago,
                    CONCAT(YEAR(#{activity_options[:by]}), CONCAT(DAYOFYEAR(#{activity_options[:by]}), HOUR(#{activity_options[:by]}))) AS unique_hour
                  FROM #{self.table_name}
                  WHERE #{activity_scope} AND #{activity_options[:by]} > ?
                  GROUP BY unique_hour
                  ORDER BY #{activity_options[:by]} ASC",
                  since.to_s(:db)
                ]
              )
            elsif db_adapter_name == 'postgresql'
               sql_statement = sanitize_sql(
                [
                  "SELECT count(*) as activity_count, hours_ago from
                (
                  SELECT 
                 ((((EXTRACT(year from now()) - EXTRACT(year from #{activity_options[:by]}))*365)  +(EXTRACT(day from now())-EXTRACT(day from #{activity_options[:by]})))*24)+(EXTRACT(hour from now())-EXTRACT(hour from #{activity_options[:by]})) as hours_ago
                  FROM #{self.table_name}
                  WHERE #{activity_scope} AND #{activity_options[:by]} > ?
                  ORDER BY #{activity_options[:by]} ASC) as temporary
                 GROUP BY temporary.hours_ago
                 ORDER BY temporary.hours_ago DESC
                  ",since.to_s(:db)])

            end
            unit = "hours_ago"
            oldest_possible_unit = ((Time.now-since)/60)/60

          when :week
            if db_adapter_name == 'mysql'
            sql_statement = sanitize_sql(
              ["SELECT                 
                  COUNT(*) AS activity_count,
                  ((YEAR(now()) - YEAR(#{activity_options[:by]}))*52)+(WEEK(now())-WEEK(#{activity_options[:by]})) as weeks_ago,
                  YEARWEEK(#{activity_options[:by]}) AS unique_week
                FROM #{self.table_name}
                WHERE #{activity_scope} AND #{activity_options[:by]} > ?
                GROUP BY unique_week
                ORDER BY #{activity_options[:by]} ASC",
                since.to_s(:db)
              ]
            )
            elsif db_adapter_name == 'postgresql'
              sql_statement = sanitize_sql(
                [
                  "SELECT count(*) as activity_count, weeks_ago from
                (
                  SELECT 
                 ((EXTRACT(year from now()) - EXTRACT(year from #{activity_options[:by]}))*52)+(EXTRACT(week from now())-EXTRACT(week from #{activity_options[:by]})) as weeks_ago
                  FROM #{self.table_name}
                  WHERE #{activity_scope} AND #{activity_options[:by]} > ?
                  ORDER BY #{activity_options[:by]} ASC) as temporary
                 GROUP BY temporary.weeks_ago
                 ORDER BY temporary.weeks_ago DESC
                  ",since.to_s(:db)])
              
            end
            
            unit = "weeks_ago"
            oldest_possible_unit = ((((Time.now-since)/60)/60)/24)/7
            
          else
            if db_adapter_name == 'mysql'             
            sql_statement = sanitize_sql(
              ["SELECT                  
                  COUNT(*) AS activity_count,
                  DATEDIFF(now(), #{activity_options[:by]}) as days_ago
                FROM #{self.table_name}
                WHERE #{activity_scope} AND #{activity_options[:by]} > ?
                GROUP BY days_ago
                ORDER BY #{activity_options[:by]} ASC",
                since.to_s(:db)
              ]
            )
            elsif db_adapter_name == 'postgresql'
              sql_statement = sanitize_sql(
                [
                  "SELECT count(*) as activity_count, days_ago from
                (
                  SELECT 
                 to_char((now() - #{activity_options[:by]}),'DDD') as days_ago
                  FROM #{self.table_name}
                  WHERE #{activity_scope} AND #{activity_options[:by]} > ?
                  ORDER BY #{activity_options[:by]} ASC) as temporary
                 GROUP BY temporary.days_ago
                 ORDER BY temporary.days_ago DESC
                  ",since.to_s(:db)])
            end
            
            unit = "days_ago"
            oldest_possible_unit = (((Time.now-since)/60)/60)/24
          end
          
          results = connection.select_all(sql_statement)
          (options[:padding] ? pad_activity_results(results, unit, oldest_possible_unit.round, options[:order]) : format_activity_results(results, unit, order))
        end
      
      private
        def format_activity_results(results, unit, order)
          results.inject([]) do |rs,r|
            entry = {
              :offset => r[unit].to_i,
              :activity => r["activity_count"].to_i            
            }
            (order == :asc) ? rs.push(entry) : rs.unshift(entry)
          end
        end
        
        def pad_activity_results(results, unit, oldest_possible_offset, order)
          padded_results = []
          
          current_unit_offset = oldest_possible_offset
          current_result_index = 0
          
          while current_unit_offset >= 0 do
            if current_result_index < results.size && results[current_result_index][unit].to_i == current_unit_offset
              entry = {
                :offset => current_unit_offset,
                :activity => results[current_result_index]["activity_count"].to_i                
              }
              current_result_index = current_result_index+1
            else
              case unit
              when "hours_ago"
                created_at_given_offset = Time.now-current_unit_offset.hours
              when "weeks_ago"
                created_at_given_offset = Time.now-current_unit_offset.weeks
              else
                created_at_given_offset = Time.now-current_unit_offset.days
              end
              entry = {
                :offset => current_unit_offset,
                :activity => 0                
              }
            end
            current_unit_offset = current_unit_offset-1
            (order == :asc) ? padded_results.push(entry) : padded_results.unshift(entry)
          end
          
          padded_results
        end
      end
      
      module InstanceMethods;end
      
    end
  end
end