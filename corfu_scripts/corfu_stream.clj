; Query endpoint status given the endpoint as the first arg
(in-ns 'org.corfudb.shell) ; so our IDE knows what NS we are using

(import org.docopt.Docopt) ; parse some cmdline opts

(def usage "corfu_stream, work with Corfu streams.
Usage:
  corfu_stream [-i <stream-id>] [-c <config>] read
  corfu_stream [-i <stream-id>] [-c <config>] write
Options:
  -i <stream-id>, --stream-id <stream-id>     ID or name of the stream to work with.
  -c <config>, --config <config>              Configuration string to use.
  -h, --help     Show this screen.
")

(def localcmd (.. (new Docopt usage) (parse *args)))

(println (.. localcmd (get "--config")))