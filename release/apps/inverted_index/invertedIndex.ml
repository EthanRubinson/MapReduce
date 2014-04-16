open Async.Std
open Async_unix

type filename = string

(******************************************************************************)
(** {2 The Inverted Index Job}                                                *)
(******************************************************************************)

module Job = struct
  type filename = string
  type input = (filename * string) 
  type key = string
  type inter = filename
  type output = inter list

  let name = "index.job"

  let map input : (key * inter) list Deferred.t =
let words (str:string):key list  = AppUtils.split_words str in

    let rec outputmaker (accumLst:(key*inter) list) (words:key list) (seenLst:key list): (key*inter) list= 
    match words with
    |[] ->  accumLst
    |hd::tl -> (if (List.mem hd seenLst) then 
      (outputmaker accumLst tl seenLst) else 
      (outputmaker ((hd,(fst input))::accumLst) 
        tl (hd::seenLst))) in
    return (outputmaker [] (words (snd input)) [])

  let reduce (key, inters) : output Deferred.t =
    let rec intercomp interslst accumList = 
    match interslst with
    |[] -> accumList
    |hd::tl -> if (List.mem hd accumList) then intercomp tl accumList else intercomp tl (hd::accumList) in
    return (intercomp inters [])
end

(* register the job *)
let () = MapReduce.register_job (module Job)


(******************************************************************************)
(** {2 The Inverted Index App}                                                *)
(******************************************************************************)

module App  = struct

  let name = "index"

  (** Print out all of the documents associated with each word *)
  let output results =
    let print (word, documents) =
      print_endline (word^":");
      List.iter (fun doc -> print_endline ("    "^doc)) documents
    in

    let sorted = List.sort compare results in
    List.iter print sorted


  (** for each line f in the master list, output a pair containing the filename
      f and the contents of the file named by f.  *)
  let read (master_file : filename) : (filename * string) list Deferred.t =
    Reader.file_lines master_file >>= fun filenames ->

    Deferred.List.map filenames (fun filename ->
      Reader.file_contents filename >>= fun contents ->
      return (filename, contents)
    )

  module Make (Controller : MapReduce.Controller) = struct
    module MR = Controller(Job)

    (** The input should be a single file name.  The named file should contain
        a list of files to index. *)
    let main args =
      if args = [] then failwith "No files provided."
      else
        (read (List.hd args))
          >>= fun inputs -> MR.map_reduce inputs
          >>| 
          output
  end
end

(* register the App *)
let () = MapReduce.register_app (module App)

