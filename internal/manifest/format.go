package manifest

// Edit is encoded as a sequence tag prefixed fields, each records
// a change to an version. For collection fields, multiple fields may
// exist with same tag numbers.

// Manifest is a file that contains list of Edits, each is written as a
// log Record.

// CURRENT is a simple text file that contains the name of the latest
// MANIFEST file ending with a end of line.
