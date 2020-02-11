const NodeRSA = require('node-rsa');
const fs = require('fs');
const key = new NodeRSA();
module.exports = (filepath, publickeypath) => {
    // Read in your private key from wherever you store it
    function checkforexistances(...files) {
        for (file of files) {
            if (!fs.existsSync(file)) {
                throw `${file} path doesn't exists.`;
            };
        }
        return true;
    }
    if (checkforexistances(filepath, publickeypath)) {
        const publickey = fs.readFileSync(publickeypath, 'utf8');
        key.importKey(publickey, 'pkcs8-public-pem');
        const encrypted = fs.readFileSync(filepath, 'utf8');
        const decrypted = key.decryptPublic(encrypted, 'utf8');
        return decrypted;
    }
}