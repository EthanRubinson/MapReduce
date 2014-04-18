open Async.Std
open Async_unix

(******************************************************************************)
(** input and output types                                                    *)
(******************************************************************************)

type id = int
type dna_type = Read | Ref

type sequence = {
  id   : id;
  kind : dna_type;
  data : string;
}

(** Indicates a matching subsequence of the given read and reference *)
type result = {
  length   : int;

  read     : id;
  read_off : int;

  ref      : id;
  ref_off  : int;
}

type infos = id * dna_type * int

(******************************************************************************)
(** file reading and writing                                                  *)
(******************************************************************************)

(** Convert a line into a sequence *)
let read_sequence line = match Str.split (Str.regexp "@") line with
  | [id; "READ"; seq] -> {id=int_of_string id; kind=Read; data=seq}
  | [id; "REF";  seq] -> {id=int_of_string id; kind=Ref;  data=seq}
  | _ -> failwith "malformed input"

(** Read in the input data *)
let read_files filenames : sequence list Deferred.t =
  if filenames = [] then failwith "No files supplied"
  else
    Deferred.List.map filenames Reader.file_lines
      >>| List.flatten
      >>| List.map read_sequence


(** Print out a single match *)
let print_result result =
  printf "read %i [%i-%i] matches reference %i [%i-%i]\n"
         result.read result.read_off (result.read_off + result.length - 1)
         result.ref  result.ref_off  (result.ref_off  + result.length - 1)

(** Write out the output data *)
let print_results results : unit =
  List.iter print_result results

(******************************************************************************)
(** Dna sequencing jobs                                                       *)
(******************************************************************************)

module Job1 = struct
  type input = sequence
  type key = string
  type inter = infos
  type output = (inter * inter) list

  let name = "dna.job1"

  let map input : (key * inter) list Deferred.t =
    match input with
      | {id = i; kind = l; data = d} -> (
                                        if String.length d < 10 then 
                                          failwith "[ERROR] Reads/Refs must have length >= 10"
                                        else
                                          let rec gen_10mer str start_index remaining = 
                                            if remaining <= -1 then
                                              []
                                            else
                                              ((String.sub str start_index 10), (i,l,start_index)) :: (gen_10mer str (start_index + 1) (remaining - 1))
                                          in
                                          return (gen_10mer d 0 ((String.length d) - 10))
                                        )

  let reduce (key, inters) : output Deferred.t =
    let cartesian l l' = List.concat (List.map (fun e -> List.map (fun e' -> (e,e')) l') l) in
    let ref_lst = List.filter (fun elem -> let (_,k,_) = elem in if k == Ref then true else false) inters in
    let read_lst = List.filter (fun elem -> let (_,k,_) = elem in if k == Read then true else false) inters in
    return (cartesian ref_lst read_lst)

end

let () = MapReduce.register_job (module Job1)



module Job2 = struct
  type input = (infos * infos) list
  type key = (int * int)
  type inter = (int * int)
  type output = result list

  let name = "dna.job2"

  let map input : (key * inter) list Deferred.t =
    let map_fun = fun elem ->
      let (i,_,o) = fst(elem) in
      let (i',_,o') = snd(elem) in
        ((i, i'), (o, o'))
    in
    return (List.map map_fun input)

  let reduce (key, inters) : output Deferred.t =
    let ref_id = fst(key) in
    let read_id = snd(key) in

    let rec gen_result base_lst acc = match base_lst with
      | [] -> acc
      | h::t -> (
                let ref_off = fst(h) in
                let read_off = snd(h) in
                let rec build_res sub_lst next_ref_off next_read_off acc' = match sub_lst with
                  | [] -> acc'
                  | h'::t' -> (if (fst h') == next_ref_off && (snd h') == next_read_off then build_res t' (next_ref_off + 1) (next_read_off + 1) (acc' @ [h']) 
                             else build_res t' next_ref_off next_read_off acc' 
                            )
                in
                let resulting_list = build_res t (ref_off+1) (read_off+1) [h] in
                let compute_result ll = {length = 9 + List.length ll; read = read_id; read_off = read_off; ref = ref_id; ref_off = ref_off} in
                gen_result (List.filter (fun elem -> if List.mem elem resulting_list then false else true) t) (acc @ [compute_result resulting_list])

              )
    in
    return (gen_result (List.sort compare inters) [])



end

let () = MapReduce.register_job (module Job2)



module App  = struct

  let name = "dna"

  module Make (Controller : MapReduce.Controller) = struct
    module MR1 = Controller(Job1)
    module MR2 = Controller(Job2)

    let run (input : sequence list) : result list Deferred.t =
      MR1.map_reduce input >>= fun x -> MR2.map_reduce (List.map (fun e -> snd e) x) >>= fun y -> return (List.flatten (List.map (fun e -> snd e) y))


    let main args =
      read_files args
        >>= run
        >>| print_results
  end
end

let () = MapReduce.register_app (module App)

