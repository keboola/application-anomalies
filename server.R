
# Load libraries
library(shiny)

# Load keboola shared library
library(keboola.shiny.lib)

shinyServer(function(input, output, session) {
    # instantiate the keboola library
    klib <- KeboolaShiny$new()
    
    keboola <- reactive({
        # start up the app
        ret <- klib$startup(list(
                            appTitle = "Anomaly Detection",
                            tables = "recipeTables",  # we will grab all the tables listed in finalResults
                            cleanData = TRUE,
                            dataToSave = processedData,
                            configCallback = configCallback,
                            description = TRUE,
                            customElements = customElements
                        ))
        # return our sourceData
        klib$sourceData()()
    })
    
    # here we can do any pre-processing necessary for our sourceData
    sourceData <- reactive({
        dataSet <- keboola()
        if (length(dataSet) == 0) {
            # sourceData hasn't loaded yet, either busy or not logged in
            NULL
        } else {
            
            dataSet$anomalies$actual_value <- as.numeric(dataSet$anomalies$actual_value)
            dataSet$anomalies$expected_value <- as.numeric(dataSet$anomalies$expected_value)
            dataSet$anomalies$run_id <- NULL
            
            # Get the axis type from the descriptor parameters
            axisType <- dataSet$descriptor$parameters$axisType
            
            # need to set different types depending on the type of axis.
            if (axisType == 'date') {
                dataSet$aggregatedData <- dataSet$tsData
                dataSet$aggregatedData$ts_var <- as.POSIXct(dataSet$aggregatedData$ts_var, tz = 'UTC')
                dataSet$anomaliesTable$ts_var <- as.POSIXct(dataSet$anomalies$ts_var, tz = 'UTC')
            } else {
                if (axisType == 'tsid') {
                    dataSet$aggregatedData <- dataSet$tsIdData
                } else if (axisType == 'id') {
                    dataSet$aggregatedData <- dataSet$idDAta
                } else {
                    stop(paste0("Unknown axis type: ", axisType))
                }
                tsType <- dataSet$descriptor$parameters$tsType
                if (!is.null(tsType) && tsType == 'string') {
                    dataSet$aggregatedData$ts_var <- as.character(dataSet$aggregatedData$ts_var)
                    dataSet$anomaliesTable$ts_var <- as.character(dataSet$anomalies$ts_var)
                } else {
                    dataSet$anomaliesTable$ts_var <- as.numeric(dataSet$anomalies$ts_var)
                    dataSet$aggregatedData$ts_var <- as.numeric(dataSet$aggregatedData$ts_var)
                }
            }
            # Set some types and remove run_id
            dataSet$aggregatedData$target_var <- as.numeric(dataSet$aggregatedData$target_var)
            dataSet$aggregatedData$run_id <- NULL
            
            # return our dataset
            dataSet
        }
    })
    
    # This observer triggers startup.
    # this is the ideal place to put input update statements as it will be triggered
    # when sourceData is fully retrieved
    observe({
        dataSet <- keboola()
    })
    
    configCallback <- function(config) {
        if ('dateRangeTsId' %in% names(config)) {
            
            updateDateRangeInput(session,'dateRangeTsId',
                                 start = config$dateRangeTsId[1],
                                 end = config$danteRangeTsId[2])
            print("updated element")
        }
        if ('dateRange' %in% names(config)) {
            updateDateRangeInput(session,'dateRange',
                                 start = config$dateRange[1],
                                 end = config$danteRange[2])    
        }
        print("callback finished")
    }
    
    # This method is used to handle
    #   rendering of plots and other custom elements in descriptor. This method
    #   is used as a callback in getDescription(), it is not called directly.
    customElements <- function(elementId, content) {
        ret <- NULL
        if (elementId == 'anomalies') {
            anomalies <- sourceData()$anomaliesTable
            anomalies <- anomalies[, c('ts_var', 'actual_value', 'expected_value')]
            unit <- sourceData()$descriptor$parameters$aggregationUnit
            if (unit == 'day') {
                anomalies$ts_var <- as.Date(anomalies$ts_var)
            } else if (unit == 'week') {
                anomalies$ts_var <- paste0(substr(anomalies$ts_var, start = 1, stop = 4), ' week:', substr(anomalies$ts_var, start = 5, stop = 6))
            } else if (unit == 'month') {
                anomalies$ts_var <- paste0(substr(anomalies$ts_var, start = 1, stop = 4), ' month:', substr(anomalies$ts_var, start = 5, stop = 6))
            } else if (unit == 'quarter') {
                anomalies$ts_var <- paste0(substr(anomalies$ts_var, start = 1, stop = 4), ' quarter:', substr(anomalies$ts_var, start = 5, stop = 5))
            }
            names(anomalies) <- c('time', 'actual_value', 'expected_value')
            unit <- sourceData()$descriptor$parameters$aggregationUnit
            if (unit == 'hour' || unit == 'minute' || unit == 'second') {
                anomalies$time <- strftime(anomalies$time, format = "%Y-%m-%d %H:%M", tz = "UTC")
            }
            ret <- DT::datatable(anomalies)
        }
        return(ret)
    }
    
    # change the UI depending on the axis type (id or date)
    output$axisType <- reactive({
        return(sourceData()$descriptor$parameters$axisType)
    })

    # range selector used in case the axis is a date/datetime axis
    output$axisDatePicker <- renderUI({
        if (is.null(sourceData())) return(NULL)
        aggregatedData <- sourceData()$aggregatedData[, 'ts_var']
        config <- klib$kfig$selectedConfig()
        if ("dateRange" %in% names(config)) {
            start <- config$dateRange[1]
            end <- config$dateRange[2]
        } else {
            start <- min(aggregatedData)
            end <- max(aggregatedData)
        }
        ret <- dateRangeInput('dateRange', 'Time range for displayed data:',
                       min = min(aggregatedData),
                       max = max(aggregatedData),
                       start = start,
                       end = end
        )
        return(list(ret))
    })

    # range selector used in case the axis is a date/datetime axis
    output$axisTsIdPicker <- renderUI({
        print("axisTsIdPicker")
        if (is.null(sourceData())) return(NULL)
        unit <- sourceData()$descriptor$parameters$aggregationUnit
        aggregatedData <- sourceData()$aggregatedData[, 'ts_var']
        minTsId <- min(aggregatedData)
        maxTsId <- max(aggregatedData)
        minDate <- NULL
        maxDate <- NULL
        if (!is.null(unit)) {
            if (unit == 'week') {
                # tsId is in form yearWeek (e.g. 201452), add 1 as the number of day in week
                # this could've been done using minDate <- as.POSIXlt(paste0(minTsId, '-1'), format = "%Y%U-%u")
                # but that would needs to start with week 0 and TsId starts with week 1
                year <- substr(minTsId, 0, 4)
                week <- substr(minTsId, 5, 6)
                minDate <- as.POSIXlt(paste(year, week, 1, sep = '-'), format = "%Y-%U-%u")
                year <- substr(maxTsId, 0, 4)
                week <- substr(maxTsId, 5, 6)
                maxDate <- as.POSIXlt(paste(year, week, 1, sep = '-'), format = "%Y-%U-%u")
            } else if (unit == 'month') {
                # tsId is in form yearMonth (e.g. 201412), add 1 as the number of day in month
                minDate <- as.POSIXlt(paste(minTsId, 1, sep = '-'), format = "%Y%m-%d")
                maxDate <- as.POSIXlt(paste(maxTsId, 1, sep = '-'), format = "%Y%m-%d")
            } else if (unit == 'quarter') {
                # tsId is in form yearQuarter (e.g. 20144), get month number and add 1 as day of month
                year <- substr(minTsId, 0, 4)
                month <- (as.integer(substr(minTsId, 5, 5)) - 1) * 3
                minDate <- as.POSIXlt(paste(year, month, 1, sep = '-'), format = "%Y-%m-%d")
                year <- substr(maxTsId, 0, 4)
                month <- (as.integer(substr(maxTsId, 5, 5)) - 1) * 3
                maxDate <- as.POSIXlt(paste(year, month, 1, sep = '-'), format = "%Y-%m-%d")
            } else if (unit == 'year') {
                # tsId is in form year (e.g. 2014), add 1 as the number of day and number of month
                minDate <- as.POSIXlt(paste(minTsId, 1, 1, sep = '-'), format = "%Y-%m-%d")
                maxDate <- as.POSIXlt(paste(maxTsId, 1, 1, sep = '-'), format = "%Y-%m-%d")
            }
        } else {
            minDate <- NULL
            maxDate <- NULL
        }
        config <- klib$kfig$selectedConfig()
        if ("dateRangeTsId" %in% names(config)) {
            start <- config$dateRangeTsId[1]
            end <- config$dateRangeTsId[2]
        } else {
            start <- minDate
            end <- maxDate
        }
        ret <- dateRangeInput('dateRangeTsId', 'Time range for displayed data:',
                       min = minDate,
                       max = maxDate,
                       start = start,
                       end = end
        )
        return(list(ret))
    })
    
    # range selector used in case the axis is an ID axis
    output$axisIdPicker <- renderUI({
        print("axisIdPicker")
        if (is.null(sourceData())) return(NULL)
        aggregatedData <- sourceData()$aggregatedData[, 'ts_var']
        print(sourceData()$descriptor)
        tsType <- sourceData()$descriptor$parameters$tsType
        if (tsType == 'string') {
            return(list(NULL))
        }
        minId <- min(aggregatedData)
        maxId <- max(aggregatedData)    
        if ("idRange" %in% names(config)) {
            start <- config$idRange[1]
            end <- config$idRange[2]
        } else {
            start <- minId
            end <- maxId
        }        
        ret <- sliderInput('idRange', 'Range for displayed data:',
                            value = c(start, end),
                            min = minId,
                            max = maxId
        )
        
        return(list(ret))
    })
    
    # Render the table with source data in the specified range
    # Not being used currently
    output$sourceTable <- renderUI({
      #  return(list(DT::datatable(sourceData()$cleanData)))
        return(NULL)
    })
    
    # get formatted data for the selected range
    processedData <- reactive({
        if (is.null(sourceData())) return(NULL)
        data <- sourceData()$aggregatedData
        unit <- sourceData()$descriptor$parameters$aggregationUnit
        axisType <- sourceData()$descriptor$parameters$axisType
        if (axisType == 'date') {
            if (unit == 'hour' || unit == 'minute' || unit == 'second') {
                data$ts_var <- as.POSIXct(data$ts_var)
                data <- data[which((data$ts_var >= as.POSIXct(input$dateRange[1])) & (data$ts_var <= as.POSIXct(input$dateRange[2]))), ]
            } else {
                data$ts_var <- as.Date(data$ts_var)
                data <- data[which((data$ts_var >= input$dateRange[1]) & (data$ts_var <= input$dateRange[2])), ]
            }
        }
        anomalies <- sourceData()$anomaliesTable
        if (axisType == 'date') {
            if (unit == 'hour' || unit == 'minute' || unit == 'second') {
                anomalies$ts_var <- as.POSIXct(anomalies$ts_var)
            } else {
                anomalies$ts_var <- as.Date(anomalies$ts_var)
            }
        }
        anomalies <- anomalies[, c('ts_var', 'expected_value')]
        data <- merge(x = data, y = anomalies, by = "ts_var", all.x = TRUE)
        if (unit == 'hour' || unit == 'minute' || unit == 'second') {
            data$ts_var <- strftime(data$ts_var, format = "%Y-%m-%d %H:%M", tz = "UTC")
        } else if (unit == 'week') {
            data$ts_var <- paste0(substr(data$ts_var, start = 1, stop = 4), ' w:', substr(data$ts_var, start = 5, stop = 6))
        } else if (unit == 'month') {
            data$ts_var <- paste0(substr(data$ts_var, start = 1, stop = 4), ' w:', substr(data$ts_var, start = 5, stop = 6))
        } else if (unit == 'quarter') {
            data$ts_var <- paste0(substr(data$ts_var, start = 1, stop = 4), ' q:', substr(data$ts_var, start = 5, stop = 5))
        }
        names(data) <- c(sourceData()$descriptor$parameters$timeVar, sourceData()$descriptor$parameters$targetVarDescription, 'expected_value')
        data
    })
    
    # Render the table with agggregated data in the specified range
    output$aggregatedTable <- renderUI({
        return(list(DT::datatable(processedData())))
    })

    # Render the table with source data in the specified range
    output$anomalyGraph <- renderPlot({
        tsType <- sourceData()$descriptor$parameters$tsType
        if (tsType == 'string') {
            return(list(NULL))
        }
            
        aggregatedData <- sourceData()$aggregatedData
        anomalies <- sourceData()$anomaliesTable
        anomalies <- anomalies[, c('ts_var', 'actual_value')]
        names(anomalies) <- c('ts_var', 'target_var')
        unit <- sourceData()$descriptor$parameters$aggregationUnit
        if (unit == 'hour' || unit == 'minute' || unit == 'second') {
            anomalies$ts_var <- as.POSIXct(anomalies$ts_var)
            aggregatedData$ts_var <- as.POSIXct(aggregatedData$ts_var)
            r1 <- as.POSIXct(input$dateRange[1])
            r2 <- as.POSIXct(input$dateRange[2])
            anomalies <- anomalies[which((anomalies$ts_var >= r1) & (anomalies$ts_var <= r2)), ]
            aggregatedData <- aggregatedData[which((aggregatedData$ts_var >= r1) & (aggregatedData$ts_var <= r2)), ]
        } else if (unit == 'day') {
            anomalies$ts_var <- as.Date(anomalies$ts_var)
            aggregatedData$ts_var <- as.Date(aggregatedData$ts_var)
            r1 <- as.Date(input$dateRange[1])
            r2 <- as.Date(input$dateRange[2])
            anomalies <- anomalies[which((anomalies$ts_var >= r1) & (anomalies$ts_var <= r2)), ]
            aggregatedData <- aggregatedData[which((aggregatedData$ts_var >= r1) & (aggregatedData$ts_var <= r2)), ]
        } else if (unit == 'week') {
            # %U is zero based and TsId week is 1 based
            tsIdMin <- paste0(format(input$dateRangeTsId[1], '%Y'), as.integer(format(input$dateRangeTsId[1], '%U')) + 1)
            tsIdMax <- paste0(format(input$dateRangeTsId[2], '%Y'), as.integer(format(input$dateRangeTsId[2], '%U')) + 1)
            anomalies <- anomalies[which((anomalies$ts_var >= tsIdMin) & (anomalies$ts_var <= tsIdMax)), ]
            aggregatedData <- aggregatedData[which((aggregatedData$ts_var >= tsIdMin) & (aggregatedData$ts_var <= tsIdMax)), ]
            aggregatedData <- aggregatedData[order(aggregatedData$ts_var),]
            anomalies <- anomalies[order(anomalies$ts_var),]
            aggregatedData$ts_var <- as.factor(aggregatedData$ts_var)
            anomalies$ts_var <- factor(anomalies$ts_var, levels = levels(aggregatedData$ts_var))
            labs <- levels(aggregatedData$ts_var)
            levels(aggregatedData$ts_var) <- paste0(substr(labs, start = 1, stop = 4), ' w:', substr(labs, start = 5, stop = 6))
            levels(anomalies$ts_var) <- paste0(substr(labs, start = 1, stop = 4), ' w:', substr(labs, start = 5, stop = 6))
        } else if (unit == 'month') {
            tsIdMin <- format(input$dateRangeTsId[1], '%Y%m')
            tsIdMax <- format(input$dateRangeTsId[2], '%Y%m')
            anomalies <- anomalies[which((anomalies$ts_var >= tsIdMin) & (anomalies$ts_var <= tsIdMax)), ]
            aggregatedData <- aggregatedData[which((aggregatedData$ts_var >= tsIdMin) & (aggregatedData$ts_var <= tsIdMax)), ]
            aggregatedData <- aggregatedData[order(aggregatedData$ts_var),]
            anomalies <- anomalies[order(anomalies$ts_var),]
            aggregatedData$ts_var <- as.factor(aggregatedData$ts_var)
            anomalies$ts_var <- factor(anomalies$ts_var, levels = levels(aggregatedData$ts_var))
            labs <- levels(aggregatedData$ts_var)
            levels(aggregatedData$ts_var) <- paste0(substr(labs, start = 1, stop = 4), ' m:', substr(labs, start = 5, stop = 6))
            levels(anomalies$ts_var) <- paste0(substr(labs, start = 1, stop = 4), ' m:', substr(labs, start = 5, stop = 6))
        } else if (unit == 'quarter') {
            tsIdMin <- paste0(format(input$dateRangeTsId[1], '%Y'), (format(input$dateRange[1], '%m') %% 3))
            tsIdMax <- paste0(format(input$dateRangeTsId[2], '%Y'), (format(input$dateRange[2], '%m') %% 3))
            anomalies <- anomalies[which((anomalies$ts_var >= tsIdMin) & (anomalies$ts_var <= tsIdMax)), ]
            aggregatedData <- aggregatedData[which((aggregatedData$ts_var >= tsIdMin) & (aggregatedData$ts_var <= tsIdMax)), ]
            aggregatedData <- aggregatedData[order(aggregatedData$ts_var),]
            anomalies <- anomalies[order(anomalies$ts_var),]
            aggregatedData$ts_var <- as.factor(aggregatedData$ts_var)
            anomalies$ts_var <- factor(anomalies$ts_var, levels = levels(aggregatedData$ts_var))
            labs <- levels(aggregatedData$ts_var)
            levels(aggregatedData$ts_var) <- paste0(substr(labs, start = 1, stop = 4), ' q:', substr(labs, start = 5, stop = 5))
            levels(anomalies$ts_var) <- paste0(substr(labs, start = 1, stop = 4), ' q:', substr(labs, start = 5, stop = 5))
        } else if (unit == 'year') {
            tsIdMin <- format(input$dateRangeTsId[1], '%Y')
            tsIdMax <- format(input$dateRangeTsId[2], '%Y')
            anomalies <- anomalies[which((anomalies$ts_var >= tsIdMin) & (anomalies$ts_var <= tsIdMax)), ]
            aggregatedData <- aggregatedData[which((aggregatedData$ts_var >= tsIdMin) & (aggregatedData$ts_var <= tsIdMax)), ]
            aggregatedData <- aggregatedData[order(aggregatedData$ts_var),]
            anomalies <- anomalies[order(anomalies$ts_var),]
            aggregatedData$ts_var <- as.factor(aggregatedData$ts_var)
            anomalies$ts_var <- factor(anomalies$ts_var, levels = levels(aggregatedData$ts_var))
        } else if (unit == '') {
            idMin <- input$idRange[1]
            idMax <- input$idRange[2]
            aggregatedData <- aggregatedData[which((aggregatedData$ts_var >= idMin) & (aggregatedData$ts_var <= idMax)), ]
            aggregatedData <- aggregatedData[order(aggregatedData$ts_var),]
            anomalies <- anomalies[which((anomalies$ts_var >= idMin) & (anomalies$ts_var <= idMax)), ]
        }
        xgraph <- ggplot2::ggplot(aggregatedData, ggplot2::aes_string(x = "ts_var", y = "target_var"))
        
        xgraph <- xgraph + ggplot2::theme_bw()
        xgraph <- xgraph + ggplot2::theme(panel.grid.major = ggplot2::element_line(colour = "gray60"), panel.grid.major.y = ggplot2::element_blank(), panel.grid.minor = ggplot2::element_blank(), text=ggplot2::element_text(size = 14))
        if (unit == 'hour' || unit == 'minute' || unit == 'second') {
            xgraph <- xgraph + ggplot2::geom_line(colour = "red")
            xgraph <- xgraph + ggplot2::scale_x_datetime(labels = function(x) { strftime(x, format = "%Y-%m-%d %H:%M", tz = "UTC") }, expand = c(0,0))
        } else if (unit == 'day') {
            print("second if")
            xgraph <- xgraph + ggplot2::geom_line(colour = "red")
            xgraph <- xgraph + ggplot2::scale_x_date(labels = function(x) { strftime(x, format = "%Y-%m-%d", tz = "UTC") }, expand = c(0,0))
            print("end second if")
        } else if ((unit == 'week') || (unit == 'month') || (unit == 'quarter') || (unit == 'year')) {
            # display at most 40 labels
            indexes <- seq(1, nrow(aggregatedData), round(nrow(aggregatedData) / 40))
            # include the last one
            indexes[[length(indexes) + 1]] <- nrow(aggregatedData)
            lev_selected <- aggregatedData[indexes, 'ts_var']
            xgraph <- xgraph + ggplot2::geom_point(colour = "red")
            xgraph <- xgraph + ggplot2::scale_x_discrete(breaks = lev_selected)
            xgraph <- xgraph + ggplot2::theme(axis.text.x = ggplot2::element_text(angle = 90))
        } else if (unit == "") {
            xgraph <- xgraph + ggplot2::geom_line(colour = "red")            
        }
        ytitle <- sourceData()$descriptor$parameters$targetVarDescription
        if (unit == '') {
            xgraph <- xgraph + ggplot2::labs(x = sourceData()$descriptor$parameters$timeVar, y = ytitle)
        } else {
            xgraph <- xgraph + ggplot2::labs(x = "time", y = ytitle)
        }
        # Add anoms to the plot as circles. Make sure it's numeric
        anomalies$target_var <- as.numeric(anomalies$target_var)
        xgraph <- xgraph + ggplot2::geom_point(data = anomalies, color = "blue", size = 4, shape = 1)
        print("done")
        return(xgraph)
    })
})
