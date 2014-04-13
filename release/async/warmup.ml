open Async.Std

let fork d f1 f2 = 
	match Deferred.peek d with
	|Some(x) -> ignore( Deferred.both (f1 x) (f2 x) )
	|None -> ()

let deferred_map l f =
  let rec map_loop = function
    | [] ->  return []
    | h_def::tl_def -> h_def >>= fun h ->
					map_loop tl_def >>= fun tl ->
					return (h::tl)
  in
   map_loop (List.map (fun x -> f x) l)