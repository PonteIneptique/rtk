RTK: Release the Krakens
========================

`Release the krakens` (RTK) is meant as a **task management scripting library** focused on retrieving data from online 
repositories and on applying a series of annotation (Segmentation with YALTAi, Clean Up, Kraken) while keeping the disk
space usage low (with some clean up function).

It provides few main classes which can be used together (see [`example.py`](example-manifests.py)).

This is currently not perfectly optimized: technically, CPU/Network bound tasks such as downloading task could have
callbacks to run GPU tasks before they are completely done...

## Installation

If you want run the script locally, run `pip install -r requirements.txt`.

If you want to run the demo files, run `quickyaltaiinstall.sh`. Models are in the early alpha release.

## Example file

See [HowTo](HOWTO.md) for a nice decision tree on how to build your own script.

See [`example.py`](example-manifests.py) which uses manifests, keeps the xml and produces TEI files.

It takes a file with a list of manifests to download from IIIF (See manifests.txt) and passes it in a suit of commands:

0. It downloads manifests and transform them into CSV files
1. It downloads images from the manifests
2. It applies YALTAi segmentation with line segmentation
3. It fixes up the image PATH of XML files
4. It processes the text as well through Kraken
5. It removes the image files (from the one hunder object that were meant to be done in group)

The batch file should be lower if you want to keep the space used low, specifically if you use DownloadIIIFManifest.

## Providing a new `Task`

A Task is defined by three main functions and one main property. See `Task` in [`rtk.task.py`](rtk/task.py).

- [Property] `._checked_files` is a **private** property which is used to pass information about items which were 
processed. Its keys are the input values of the `Task`, their associated value is a boolean indicating if this was 
processed. *It should not be accessed externally !*
- [Method] `.check()` returns a boolean indicating if everything was treated or not. `.check()` has the responsability
to fill boolean values of `._checked_files`
- [Method] `._process(inputs)` treats the inputs files (like downloading, parsing, annotating)
- [PropertyMethod] `@property .output_files` provides a list with all items which needs to be passed to the next Task

Task can have custom parameters, check [`rtk.task.py`](rtk/task.py)
