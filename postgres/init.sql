CREATE TABLE public.property_rent (
    listing_id text NULL,
    title text NULL,
    price float4 NULL,
    property_url text NULL,
    listing_date date NULL,
    municipality varchar(100) NULL,
    neighborhood varchar(100) NULL,
    "state" varchar(5) NULL,
    region varchar(50) NULL,
    bathrooms int4 NULL,
    condominium_price float4 NULL,
    condominium_details _varchar NULL,
    property_details _varchar NULL,
    iptu float4 NULL,
    bedrooms int4 NULL,
    "type" _varchar NULL,
    parking int4 NULL,
    area float4 NULL,
    scraping_date date NULL
);

CREATE UNIQUE INDEX idx_rent_listing_id ON property_rent (listing_id);
CREATE INDEX idx_rent_listing_date ON property_rent (listing_date);
CREATE INDEX idx_rent_price ON property_rent (price);

CREATE TABLE public.property_sale (
    listing_id text NULL,
    title text NULL,
    price float4 NULL,
    property_url text NULL,
    listing_date date NULL,
    municipality varchar(100) NULL,
    neighborhood varchar(100) NULL,
    "state" varchar(5) NULL,
    region varchar(50) NULL,
    bathrooms int4 NULL,
    condominium_price float4 NULL,
    condominium_details _varchar NULL,
    property_details _varchar NULL,
    iptu float4 NULL,
    bedrooms int4 NULL,
    "type" _varchar NULL,
    parking int4 NULL,
    area float4 NULL,
    scraping_date date NULL
);

CREATE UNIQUE INDEX idx_sale_listing_id ON property_sale (listing_id);
CREATE INDEX idx_sale_listing_date ON property_sale (listing_date);
CREATE INDEX idx_sale_price ON property_sale (price);

CREATE OR REPLACE FUNCTION public.prevent_duplicate_entry_rent()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$
DECLARE
    existing_id text;
    existing_type varchar(100)[]; 
BEGIN

    SELECT listing_id, "type"
    INTO existing_id, existing_type
    FROM public.property_rent
    WHERE listing_id = NEW.listing_id;

    IF existing_id IS NOT NULL THEN
        IF (existing_type @> NEW.type or 'Padrão' = ANY(NEW.type)) THEN
            RETURN NULL;
        ELSE
            IF 'Padrão' = ANY(existing_type) THEN
                existing_type := NEW.type;
            ELSE
                existing_type := existing_type || NEW.type;
            END IF;

            UPDATE public.property_rent
            SET "type" = existing_type
            WHERE listing_id = existing_id;

            RETURN NULL;
        END IF;
    END IF;

    RETURN NEW;
END;
$function$
;

CREATE OR REPLACE FUNCTION public.prevent_duplicate_entry_sale()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$
DECLARE
    existing_id text;
    existing_type varchar(100)[]; 
BEGIN

    SELECT listing_id, "type"
    INTO existing_id, existing_type
    FROM public.property_sale
    WHERE listing_id = NEW.listing_id;

    IF existing_id IS NOT NULL THEN
        IF (existing_type @> NEW.type or 'Padrão' = ANY(NEW.type)) THEN
            RETURN NULL;
        ELSE
            IF 'Padrão' = ANY(existing_type) THEN
                existing_type := NEW.type;
            ELSE
                existing_type := existing_type || NEW.type;
            END IF;

            UPDATE public.property_sale
            SET "type" = existing_type
            WHERE listing_id = existing_id;

            RETURN NULL;
        END IF;
    END IF;

    RETURN NEW;
END;
$function$
;

create trigger prevent_duplicate_entry_trigger_sale before
insert
    on
    public.property_sale for each row execute function prevent_duplicate_entry_sale();

create trigger prevent_duplicate_entry_trigger_rent before
insert
    on
    public.property_rent for each row execute function prevent_duplicate_entry_rent();