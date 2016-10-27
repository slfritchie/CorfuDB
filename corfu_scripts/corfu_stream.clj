; Query endpoint status given the endpoint as the first arg
(in-ns 'org.corfudb.shell) ; so our IDE knows what NS we are using

(import org.docopt.Docopt) ; parse some cmdline opts

(def usage "corfu_stream, work with Corfu streams.
Usage:
  corfu_stream [-i <stream-id>] -c <config> read
  corfu_stream [-i <stream-id>] -c <config> write
Options:
  -i <stream-id>, --stream-id <stream-id>     ID or name of the stream to work with.
  -c <config>, --config <config>              Configuration string to use.
  -h, --help     Show this screen.
")

; a function which reads a stream to stdout
(defn read-stream [stream] (doseq [obj (.. stream (readTo Long/MAX_VALUE))]
                             (println "a"))
                             ))

; a function which writes to a stream from stdin
(defn write-stream [stream] (let [in (slurp *in*)]
                              (.. stream (write in))
                              ))

; a function which takes a string and parses it to UUID if it is not a UUID
(defn uuid-from-string [string-param] (try (java.util.UUID/fromString string-param)
          (catch Exception e (org.corfudb.runtime.CorfuRuntime/getStreamID string-param))))

(def localcmd (.. (new Docopt usage) (parse *args)))

(get-runtime (.. localcmd (get "--config")))
(connect-runtime)

(def stream (get-stream (uuid-from-string (.. localcmd (get "--stream-id")))))

; determine whether to read or write
(cond (.. localcmd (get "read")) (read-stream stream)
      (.. localcmd (get "write")) (write-stream stream)
  :else (println "Unknown arguments.")
  )
