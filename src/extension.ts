'use strict';
// The module 'vscode' contains the VS Code extensibility API
// Import the module and reference it with the alias vscode in your code below
import * as vscode from 'vscode';


import { SimpleDF } from './simpleOne';

// this method is called when your extension is activated
// your extension is activated the very first time the command is executed
export function activate(context: vscode.ExtensionContext) {



    context.subscriptions.push(vscode.commands.registerCommand('extension.SimpleDF', () => {
        // The code you place here will be executed every time your command is executed

        // tslint:disable-next-line:no-unused-expression
        new SimpleDF(context); 
    
        })
        );
}

// this method is called when your extension is deactivated
export function deactivate() {
}