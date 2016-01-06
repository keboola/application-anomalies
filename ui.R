# UI.R -- shiny frontend

library("keboola.shiny.lib")

shinyUI(
    keboolaPage(
        fluidPage(
            column(3,
                   wellPanel(
                       div(style = "visibility: hidden",
                           textOutput("axisType")
                       ),
                       conditionalPanel(
                           condition = "output.axisType == 'date'",
                           uiOutput("axisDatePicker")
                       ),
                       conditionalPanel(
                           condition = "output.axisType == 'tsid'",
                           uiOutput("axisTsIdPicker")
                       ),
                       conditionalPanel(
                           condition = "output.axisType == 'id'",
                           uiOutput("axisIdPicker")
                       )
                   )
            ),                
            column(9,
               tabsetPanel(type = "tabs", id = 'tabsPanel', 
                   tabPanel("Description",
                            uiOutput("description")
                   ),
                   tabPanel("Graph and  data",
                            uiOutput("sourceTable"),
                            plotOutput("anomalyGraph"),
                            uiOutput("aggregatedTable"),
                            value = "tableTab"
                   )
               )
            )
        ), appTitle = "Anomaly Detection"
    )
)