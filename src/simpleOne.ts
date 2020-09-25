'use strict';
// The module 'vscode' contains the VS Code extensibility API
// Import the module and reference it with the alias vscode in your code below
import * as vscode from 'vscode';
import * as azdata from 'azdata';


interface ICommand {
    src_conn: string;
    dest_conn: string;
    src_sql_command: string;
    dst_schema: string;
    dst_table: string;
    dest_truncate: string;
    [key: string]: string;

}


export class SimpleDF {

    private connections: Map<string,string>;
    private currentConnId: string;
    private connectionNamesSource!: azdata.DropDownComponent;
    private connectionNamesDestination!: azdata.DropDownComponent;
    
    private textBoxQuery!: azdata.InputBoxComponent;
    private dstSchema!: azdata.DropDownComponent;
    private dstTable!: azdata.DropDownComponent;
    private dstTruncate!: azdata.DropDownComponent;
    private vsCodeContext: vscode.ExtensionContext;
    private schema_: string;
    private table_: string;

    private dialog!: azdata.window.Dialog;


    constructor(context: vscode.ExtensionContext) {
        this.vsCodeContext = context;
        this.openDialog();
        this.connections = new Map();
        this.currentConnId = '';
        this.schema_ ='';
        this.table_ = '';

        
        
    }

    private async getConnections(): Promise < Map<string,string> > {
        let availableConnections = await azdata.connection.getConnections(false);
        let connections: Map<string,string> = new Map;
    
        
        for (let index = 0; index < availableConnections.length; index++) {
            const element = availableConnections[index];
            if (element.databaseName !== "master" && element.databaseName !== "model" && element.databaseName !== "msdb" && element.databaseName !== "tempdb") {
                let connection_ = `Server=${element.serverName};Database=${element.databaseName};Integrated Security=True;`;
                connections.set(connection_, element.connectionId);
            }
        }
    
        if (connections.size < 1 ) {
            vscode.window.showWarningMessage(" You need to make a connecton to your database to be able to see or edit data!");   
            return new Map<string, string>();
        }
    
        return connections;
    }




    private async getSchemas(conn: string): Promise<string[]> {
        let provider: azdata.QueryProvider = azdata.dataprotocol.getProvider < azdata.QueryProvider > ("MSSQL", azdata.DataProviderType.QueryProvider);
        let defaultUri = await azdata.connection.getUriForConnection(conn);

        let query = `SELECT distinct TABLE_SCHEMA from INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE='BASE TABLE' ORDER by 1 DESC`;

        var data;
        try 
        {
            data = await provider.runQueryAndReturn(defaultUri, query);

        } catch (error) 
        {
            //azdata.window.closeDialog(this.dialog);
            vscode.window.showWarningMessage("Note: Connect to Destination in Side Bar to see list of Tables and Schemas");

            //vscode.window.showErrorMessage(error.message);
            return[];
        }

        let rows = data.rows;
        let values: Array < string > = [''];
        rows.forEach(row => values.push(row[0].displayValue));

        return values;

        
    }
    private async getTables(conn: string, schema: string): Promise<string[]> {
        let provider: azdata.QueryProvider = azdata.dataprotocol.getProvider < azdata.QueryProvider > ("MSSQL", azdata.DataProviderType.QueryProvider);
        let defaultUri = await azdata.connection.getUriForConnection(conn);

        let query = `SELECT TABLE_NAME from INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE='BASE TABLE' and TABLE_SCHEMA='${schema}' ORDER by 1 DESC`;

        var data;
        try 
        {
            data = await provider.runQueryAndReturn(defaultUri, query);

        } catch (error) 
        {
            return[];
        }

        let rows = data.rows;
        let values: Array < string > = [''];
        rows.forEach(row => values.push(row[0].displayValue));

        return values;

        
    }


    private runProjectTerminal(): void {

        let commandMap: ICommand = {
            'src_conn': this.connectionNamesSource.value as string,
            'dest_conn': this.connectionNamesDestination.value as string,
            'src_sql_command': this.textBoxQuery.value as string,
            'dst_schema': this.schema_ as string,
            'dst_table': this.table_ as string,
            'dest_truncate': this.dstTruncate.value as string
        }; 

        if(commandMap.dest_conn === '' || commandMap.dst_schema ==='' || commandMap.dst_schema === undefined || commandMap.dst_table === undefined || commandMap.dst_table === '' || commandMap.dest_truncate ==='' || commandMap.src_conn === '' || commandMap.src_sql_command === ''){
            vscode.window.showWarningMessage("Not all values are properly selected");
            return;
        }
        
        let path2 = `${this.vsCodeContext.extensionPath}\\scripts`;
        console.log(path2);

            let commandAutoGenerated = `pwsh -file "${path2}\\simple_dataflow.ps1"`;

            for(let key in commandMap){
                let value = commandMap[key];
                if (value) {
                    commandAutoGenerated = `${commandAutoGenerated} -${key} "${value}"`;
                }
            }

            // let command = `eltsnap_runtime_v2 -server "${server}" -database "${database}" -project "${this.project}" -environment "${this.selectedEnvironment}" -packages "${this.selectedPackage}" -template "${this.selectedEnvironment}"`;
            
            var activeTerminals = vscode.window.terminals;


            if(activeTerminals.length === 0){
                var terminal =vscode.window.createTerminal();

            }else
            {
                terminal = activeTerminals[0];

            }

                terminal.show();
                terminal.sendText(commandAutoGenerated, false);
                vscode.env.clipboard.writeText(commandAutoGenerated);
                vscode.window.showInformationMessage("Command copied to the clipboard and active terminal");
                azdata.window.closeDialog(this.dialog);
            

        } 

    




    private openDialog(): void {

        let dialogTitle: string = 'eltsnap: Simple Dataflow';
        this.dialog = azdata.window.createModelViewDialog(dialogTitle);
        let packagesTab = azdata.window.createTab('eltsnap: Simple Dataflow');

        packagesTab.content = 'getpackage';
        this.dialog.content = [packagesTab];

        this.dialog.okButton.hidden = true;

        let runDF = azdata.window.createButton('Place Command in Terminal Window');
        runDF.onClick(() => this.runProjectTerminal());

        this.dialog.customButtons = [runDF];

        packagesTab.registerContent(async (view) => {
            await this.getTabContent(view, 400);
        });

        azdata.window.openDialog(this.dialog);
    }
    
    private async getTabContent(view: azdata.ModelView, componentWidth: number): Promise < void > {
        this.connections = await this.getConnections();
        
        let connectionNames = Array.from(this.connections.keys());
        connectionNames.unshift('');
        this.connectionNamesSource = view.modelBuilder.dropDown().withProperties({ values : connectionNames}).component();
        this.connectionNamesDestination = view.modelBuilder.dropDown().withProperties({values : connectionNames}).component();

        this.textBoxQuery = view.modelBuilder.inputBox().withProperties({editable: true,multiline: true, rows:3, values:['SELECT * FROM {select_table}' ]}).component();           
        this.dstSchema = view.modelBuilder.dropDown().withProperties({editable: true, values:[], fireOnTextChange: true}).component();           
        this.dstTable = view.modelBuilder.dropDown().withProperties({editable: true, values:[], fireOnTextChange: true}).component();           
        this.dstTruncate = view.modelBuilder.dropDown().withProperties({values: ['N','Y']}).component();           

        this.connectionNamesDestination.onValueChanged( async e => {
            this.currentConnId = this.connections.get(e.selected) as string;

            await this.dstSchema.updateProperties({values: [], value: ''});
            

            await this.dstTable.updateProperties({values: [], value: ''});


            this.getSchemas(this.currentConnId as string).then( (m) => this.dstSchema.values = m );

        });

        this.dstSchema.onValueChanged(e => {
            this.dstTable.value = '';
            this.dstTable.values = [];

            this.getTables(this.currentConnId as string,e as string).then( (m) => this.dstTable.values = m );
            this.schema_ = e;
        });

        this.dstTable.onValueChanged(e => {
            this.table_ =e;
        });

        let formBuilder = view.modelBuilder.formContainer()
        .withFormItems([
            {
                components:[
                    {
                        component: this.textBoxQuery,
                        title: "Source Select"
                    },    
                    {
                        component: this.dstSchema,
                        title: "Destination schema"
                    },    
                    {
                        component: this.dstTable,
                        title: "Destination table"
                    },    
                    {
                        component: this.dstTruncate,
                        title: "Truncate Destination Table"
                    },    
                    ],
            title: " "
            }],{ horizontal: true,
                componentWidth: 250,
                titleFontSize: 11
            }
        ).component();
 

        let formBuilder1 = view.modelBuilder.formContainer()
        .withFormItems([
            {
                components:[
                    {
                        component: this.connectionNamesSource,
                        title: "Source Connection String"
                    }, 
                    {
                        component: this.connectionNamesDestination,
                        title: "Destination Connection String"
                    },
                                ],
            title: " "
            },
        ],{
            horizontal: true,
            componentWidth: 300,
            titleFontSize: 11
        }
        ).component();


        let formBuilder0 = view.modelBuilder.formContainer()
        .withFormItems([
            {
                components:[
                    
                            ],
        title: "Define Dataflow Source and Destination"
            },
        ]
        ).component();

        

        let formBuilder02 = view.modelBuilder.formContainer()
        .withFormItems([
            {
                components:[
                    
                            ],
        title: "Configure Dataflow"
            },
        ]
        ).component();


    let groupModel1 = view.modelBuilder.groupContainer()
        .withLayout({}).withItems([formBuilder0, formBuilder1, formBuilder02, formBuilder]).component();

    await view.initializeModel(groupModel1);

}



    }