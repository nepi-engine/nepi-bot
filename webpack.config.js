const path = require('path');
const nodeExternals = require('webpack-node-externals');

module.exports = {
    mode: "production",
    target: "node",
    entry: {
        app: ["./src/index.js"]
    },
    output: {
        path: path.resolve(__dirname, "./build"),
        filename: "index.js"
    },
    externals: [nodeExternals()],
};
